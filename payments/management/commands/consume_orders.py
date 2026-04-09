"""
Management command : consommateur Kafka pour order_detail
Usage : python manage.py consume_orders

Lit le topic order_detail, crée un paiement simulé,
publie order_confirmed, crée une notification.
"""
import json
import uuid
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Consomme le topic Kafka order_detail et traite les paiements'

    def handle(self, *args, **options):
        try:
            from kafka import KafkaConsumer
        except ImportError:
            self.stderr.write('kafka-python non installé. pip install kafka-python')
            return

        self.stdout.write(self.style.SUCCESS(
            f'[Consumer] Écoute topic: {settings.KAFKA_TOPICS["ORDER_DETAIL"]}'
        ))

        consumer = KafkaConsumer(
            settings.KAFKA_TOPICS['ORDER_DETAIL'],
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='payments-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        for message in consumer:
            payload = message.value
            self.stdout.write(f'[Consumer] Reçu: {payload}')
            self._process(payload)

    def _process(self, payload):
        from orders.models import Order
        from payments.models import Payment
        from notifications.views import send_notification
        from config.kafka_utils import publish

        order_id = payload.get('order_id')
        if not order_id:
            return

        try:
            order = Order.objects.get(pk=order_id)
        except Order.DoesNotExist:
            logger.warning(f'Order {order_id} not found')
            return

        # Évite le double traitement
        if hasattr(order, 'payment'):
            return

        # Crée le paiement
        payment = Payment.objects.create(
            order=order,
            reference=f'PAY-{uuid.uuid4().hex[:10].upper()}',
            amount=order.total_amount,
            method='card',
            status='confirmed',
        )

        # Met à jour la commande
        order.status = 'confirmed'
        order.confirmed_at = timezone.now()
        order.save()

        # Publie order_confirmed
        publish(
            settings.KAFKA_TOPICS['ORDER_CONFIRMED'],
            {
                'order_id':       order.id,
                'payment_ref':    payment.reference,
                'customer_email': order.customer_email,
                'customer_name':  order.customer_name,
                'total_amount':   str(order.total_amount),
                'confirmed_at':   order.confirmed_at.isoformat(),
            }
        )

        self.stdout.write(self.style.SUCCESS(
            f'[Consumer] Order #{order_id} confirmé → order_confirmed publié'
        ))