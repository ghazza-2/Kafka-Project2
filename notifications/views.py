from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.core.paginator import Paginator

from orders.models import Order
from .models import Notification


def notification_list(request):
    qs = Notification.objects.select_related('order').all()

    ntype = request.GET.get('type', '')
    if ntype:
        qs = qs.filter(type=ntype)

    paginator     = Paginator(qs, 25)
    notifications = paginator.get_page(request.GET.get('page'))
    unread_count  = Notification.objects.filter(is_read=False).count()

    return render(request, 'notifications/notification_list.html', {
        'notifications': notifications,
        'unread_count':  unread_count,
    })


def send_notification(order_id: int):
    """
    Crée et simule l'envoi d'une notification pour une commande confirmée.
    Appelé par le consumer Kafka (management command) qui lit order_confirmed.
    """
    try:
        order = Order.objects.get(pk=order_id)
    except Order.DoesNotExist:
        return

    notif = Notification.objects.create(
        order=order,
        type='email',
        title=f'Commande #{order.id} confirmée !',
        message=(
            f"Bonjour {order.customer_name},\n"
            f"Votre commande #{order.id} d'un montant de {order.total_amount} DT "
            f"a été confirmée. Livraison à : {order.delivery_address}."
        ),
        recipient=order.customer_email,
        sent=True,
    )

    order.notification_sent = True
    order.save(update_fields=['notification_sent'])

    return notif