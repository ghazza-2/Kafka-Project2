"""
management command : transaction_consumer
=========================================
Adapté de transaction.py — tourne en arrière-plan.

Lit topic  : order_details
Publie sur : order_confirmed
Met à jour en DB : Order.status = 'processing' puis crée Payment

Lancer :  python manage.py transaction_consumer
"""
import json
import uuid
import logging

from django.core.management.base import BaseCommand
from django.utils import timezone

from orders.models import Order

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Consumer Kafka : order_details → traite paiement → order_confirmed'

    def handle(self, *args, **options):
        try:
            from kafka import KafkaConsumer, KafkaProducer
        except ImportError:
            self.stderr.write('kafka-python non installé.')
            return

        from django.conf import settings
        from orders.models import Order
        from payments.models import Payment

        bootstrap = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

        # ── Consumer (comme transaction.py) ──────────────────────────────────
        consumer = KafkaConsumer(
            'order_details',
            bootstrap_servers=bootstrap,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='transaction-consumer-group-v2',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        # ── Producer (comme transaction.py) ──────────────────────────────────
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        self.stdout.write(self.style.SUCCESS(
            '[transaction_consumer] En écoute sur order_details…'
        ))

        for message in consumer:
            try:
                payload = message.value
                self.stdout.write(f'\n[transaction_consumer] Ongoing transaction..')
                self.stdout.write(f'  Message: {payload}')

                # ── Récupère les données (format producer.py) ─────────────
                order_id       = payload.get('order_id')
                user_id        = payload.get('user_id', '')
                total_cost     = float(payload.get('total_cost', 0))
                customer_email = payload.get('customer_email') or f'{user_id}@gmail.com'
                customer_name  = payload.get('customer_name') or user_id

                # ── Met à jour la commande Django en DB ───────────────────
                try:
                    order = Order.objects.get(pk=order_id)
                    order.status = 'processing'
                    order.save(update_fields=['status'])
                    self.stdout.write(f'  ✓ Order #{order_id} → processing')
                except Order.DoesNotExist:
    # Créer la commande depuis le payload Kafka
                    order = Order.objects.create(
                    id=order_id,
                    customer_name=customer_name,
                    customer_email=customer_email,
                    delivery_address=payload.get('delivery_address', 'Non renseigné'),
                    total_amount=total_cost,
                    notes=payload.get('notes', ''),
                    status='processing',
                    )
                    self.stdout.write(f'  ✓ Order #{order_id} créée en DB depuis Kafka')

                # ── Crée le paiement (simulé comme confirmé) ──────────────
                if order and not hasattr(order, 'payment'):
                    try:
                        Payment.objects.create(
                            order=order,
                            reference=f'PAY-{uuid.uuid4().hex[:10].upper()}',
                            amount=order.total_amount,
                            method='card',
                            status='confirmed',
                        )
                        order.status = 'confirmed'
                        order.confirmed_at = timezone.now()
                        order.save(update_fields=['status', 'confirmed_at'])
                        self.stdout.write(f'  ✓ Payment créé → Order #{order_id} → confirmed')
                    except Exception as e:
                        logger.warning(f'Payment creation error: {e}')

                # ── Publie order_confirmed (format transaction.py) ────────
                confirmed_data = {
                    'order_id':      order_id,
                    'customer_id':   user_id,
                    'customer_email': customer_email,
                    'customer_name': customer_name,
                    'total_cost':    total_cost,
                }
                producer.send('order_confirmed', confirmed_data)
                producer.flush()

                self.stdout.write(
                    self.style.SUCCESS(f'  ✓ Publié sur order_confirmed : order #{order_id}')
                )

            except Exception as e:
                logger.error(f'[transaction_consumer] Erreur: {e}')
                self.stderr.write(f'  ✗ Erreur: {e}')