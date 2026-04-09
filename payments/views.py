import uuid
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.db.models import Sum, Count
from django.utils import timezone

from orders.models import Order
from .models import Payment
from config.kafka_utils import publish
from django.conf import settings


def payment_list(request):
    payments = Payment.objects.select_related('order').all()

    stats = {
        'total':     payments.count(),
        'confirmed': payments.filter(status='confirmed').count(),
        'failed':    payments.filter(status='failed').count(),
        'volume':    payments.filter(status='confirmed').aggregate(
                         s=Sum('amount'))['s'] or 0,
    }

    return render(request, 'payments/payment_list.html', {
        'payments': payments,
        'stats':    stats,
    })


def process_payment(request, order_id):
    """
    Simule le traitement d'un paiement pour une commande.
    En production, appelé par le consumer Kafka qui lit order_detail.
    """
    order = get_object_or_404(Order, pk=order_id)

    if hasattr(order, 'payment'):
        messages.warning(request, 'Ce paiement a déjà été traité.')
        return redirect('orders:detail', pk=order_id)

    # Crée le paiement (simulé comme confirmé)
    payment = Payment.objects.create(
        order=order,
        reference=f"PAY-{uuid.uuid4().hex[:10].upper()}",
        amount=order.total_amount,
        method=request.POST.get('method', 'card'),
        status='confirmed',
    )

    # Met à jour le statut de la commande
    from django.utils import timezone
    order.status = 'confirmed'
    order.confirmed_at = timezone.now()
    order.save()

    # Publie order_confirmed sur Kafka
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

    messages.success(request, f'Paiement {payment.reference} confirmé → order_confirmed publié sur Kafka.')
    return redirect('orders:detail', pk=order_id)