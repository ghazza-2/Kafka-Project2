"""
management command : analytics_consumer
========================================
Adapté de analytics.py — tourne en arrière-plan.

Lit topic  : order_confirmed
Actions    : 
  - Compte les commandes et revenus (comme analytics.py)
  - Sauvegarde OrderEvent en DB pour le dashboard SSE
  - Met à jour Order.status = 'confirmed' si pas déjà fait

Lancer : python manage.py analytics_consumer
"""
import json
import logging

from django.core.management.base import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Consumer Kafka : order_confirmed → met à jour analytics + DB'

    def handle(self, *args, **options):
        try:
            from kafka import KafkaConsumer
        except ImportError:
            self.stderr.write('kafka-python non installé.')
            return

        from django.conf import settings
        from analytics.models import OrderEvent
        from orders.models import Order

        bootstrap = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

        consumer = KafkaConsumer(
            'order_confirmed',
            bootstrap_servers=bootstrap,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='analytics-consumer-group-v2',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        # Compteurs en mémoire (comme analytics.py)
        total_orders_count = 0
        total_revenue = 0

        self.stdout.write(self.style.SUCCESS(
            '[analytics_consumer] Gonna start listening on order_confirmed…'
        ))

        for message in consumer:
            try:
                consumed_message = message.value
                self.stdout.write(f'\n[analytics_consumer] Updating analytics..')

                # ── Extraction (format transaction.py) ─────────────────────
                order_id      = consumed_message.get('order_id', 0)
                total_cost    = float(consumed_message.get('total_cost', consumed_message.get('total_amount', 0)))
                customer_name = consumed_message.get('customer_name', consumed_message.get('customer_id', 'Inconnu'))
                customer_email = consumed_message.get('customer_email', '')

                # ── Compteurs en mémoire (comme analytics.py) ─────────────
                total_orders_count += 1
                total_revenue += total_cost
                self.stdout.write(f'  Orders so far today: {total_orders_count}')
                self.stdout.write(f'  Revenue so far today: {total_revenue}')

                # ── Persiste OrderEvent en DB pour le dashboard ────────────
                OrderEvent.objects.create(
                    order_id=order_id,
                    topic='order_confirmed',
                    defaults={
                        'customer_name': customer_name,
                        'total_amount':  total_cost,
                    }
                )

                # ── S'assure que la commande Django est bien confirmée ─────
                if order_id:
                    try:
                        order = Order.objects.get(pk=order_id)
                        if order.status != 'confirmed':
                            order.status = 'confirmed'
                            order.confirmed_at = order.confirmed_at or timezone.now()
                            order.save(update_fields=['status', 'confirmed_at'])
                            self.stdout.write(f'  ✓ Order #{order_id} → confirmed (DB)')
                    except Order.DoesNotExist:
                        pass

                self.stdout.write(
                    self.style.SUCCESS(f'  ✓ Analytics mis à jour (order #{order_id})')
                )

            except Exception as e:
                logger.error(f'[analytics_consumer] Erreur: {e}')
                self.stderr.write(f'  ✗ Erreur: {e}')