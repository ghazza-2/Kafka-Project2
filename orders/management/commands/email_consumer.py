"""
management command : email_consumer
=====================================
Adapté de email.py — tourne en arrière-plan.

Lit topic : order_confirmed
Actions   :
  - Simule l'envoi d'email (comme email.py)
  - Crée Notification en DB
  - Met à jour Order.notification_sent = True

Lancer : python manage.py email_consumer
"""
import json
import logging

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Consumer Kafka : order_confirmed → envoie email/notification"

    def handle(self, *args, **options):
        try:
            from kafka import KafkaConsumer
        except ImportError:
            self.stderr.write('kafka-python non installé.')
            return

        from django.conf import settings
        from orders.models import Order
        from notifications.models import Notification

        bootstrap = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

        consumer = KafkaConsumer(
            'order_confirmed',
            bootstrap_servers=bootstrap,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='email-consumer-group-v2',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        # Set d'emails (comme email.py)
        emails_sent_so_far = set()

        self.stdout.write(self.style.SUCCESS(
            '[email_consumer] Gonna start listening on order_confirmed…'
        ))

        for message in consumer:
            try:
                consumed_message = message.value

                customer_email = consumed_message.get('customer_email', '')
                customer_name  = consumed_message.get('customer_name', consumed_message.get('customer_id', 'Client'))
                order_id       = consumed_message.get('order_id', 0)
                total_cost     = float(consumed_message.get('total_cost', consumed_message.get('total_amount', 0)))

                # ── Simule l'envoi email (comme email.py) ─────────────────
                self.stdout.write(f'\n[email_consumer] Sending email to {customer_email}')
                emails_sent_so_far.add(customer_email)
                self.stdout.write(
                    f'  So far emails sent to {len(emails_sent_so_far)} unique emails'
                )

                # ── Crée Notification en DB ───────────────────────────────
                if order_id:
                    try:
                        order = Order.objects.get(pk=order_id)

                        # Evite les doublons
                        if not Notification.objects.filter(order=order, type='email').exists():
                            Notification.objects.create(
                                order=order,
                                type='email',
                                title=f'Commande #{order.id} confirmée !',
                                message=(
                                    f"Bonjour {order.customer_name},\n"
                                    f"Votre commande #{order.id} d'un montant de "
                                    f"{order.total_amount} DT a été confirmée.\n"
                                    f"Livraison à : {order.delivery_address}."
                                ),
                                recipient=order.customer_email,
                                sent=True,
                            )

                        order.notification_sent = True
                        order.save(update_fields=['notification_sent'])

                        self.stdout.write(
                            self.style.SUCCESS(f'  ✓ Notification créée + Order #{order_id}.notification_sent = True')
                        )
                    except Order.DoesNotExist:
                        # Commande pas en DB Django (créée par producer.py standalone)
                        self.stdout.write(f'  ⚠ Order #{order_id} pas en DB Django — email simulé seulement')

            except Exception as e:
                logger.error(f'[email_consumer] Erreur: {e}')
                self.stderr.write(f'  ✗ Erreur: {e}')