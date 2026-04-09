"""
orders/views.py

Quand une commande est créée :
  1. On sauve en DB
  2. On publie sur Kafka → order_details  (exactement comme producer.py)

Les consumers (transaction.py, analytics.py, email.py) s'occupent du reste.
"""
import json
from decimal import Decimal

from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.core.paginator import Paginator
from django.db.models import Sum, Q
from django.utils import timezone
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import Order, OrderItem


# ── Kafka producer helper ─────────────────────────────────────────────────────
def _get_kafka_producer():
    try:
        from kafka import KafkaProducer
        from django.conf import settings
        return KafkaProducer(
            bootstrap_servers=getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Kafka producer unavailable: {e}")
        return None


def _publish_order_details(order):
    """
    Publie sur order_details — même format que producer.py :
    {
        "order_id": i,
        "user_id": "tom_i",
        "total_cost": i,
        "items": "burger,sandwich",
    }
    """
    producer = _get_kafka_producer()
    if not producer:
        return False

    item_names = ",".join(item.name for item in order.items.all())
    data = {
        "order_id":   order.id,
        "user_id":    order.customer_email,          # email comme identifiant
        "total_cost": float(order.total_amount),
        "items":      item_names or "commande",
        # Champs supplémentaires pour les consumers Django
        "customer_name":    order.customer_name,
        "customer_email":   order.customer_email,
        "delivery_address": order.delivery_address,
    }

    try:
        producer.send("order_details", json.dumps(data).encode("utf-8"))
        producer.flush()
        import logging
        logging.getLogger(__name__).info(f"[Kafka] → order_details: order #{order.id}")
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[Kafka] publish error: {e}")
        return False


# ── Views ─────────────────────────────────────────────────────────────────────

def order_list(request):
    qs = Order.objects.all()

    q = request.GET.get('q', '').strip()
    if q:
        qs = qs.filter(
            Q(customer_name__icontains=q) |
            Q(customer_email__icontains=q)
        )

    status = request.GET.get('status', '')
    if status:
        qs = qs.filter(status=status)

    paginator = Paginator(qs, 20)
    orders = paginator.get_page(request.GET.get('page'))

    today = timezone.now().date()
    stats = {
        'total':     Order.objects.count(),
        'pending':   Order.objects.filter(status='pending').count(),
        'confirmed': Order.objects.filter(status='confirmed', created_at__date=today).count(),
        'revenue':   Order.objects.filter(status='confirmed').aggregate(
                         s=Sum('total_amount'))['s'] or 0,
    }

    return render(request, 'orders/order_list.html', {'orders': orders, 'stats': stats})


def order_create(request):
    if request.method == 'POST':
        customer_name    = request.POST.get('customer_name', '').strip()
        customer_email   = request.POST.get('customer_email', '').strip()
        delivery_address = request.POST.get('delivery_address', '').strip()
        notes            = request.POST.get('notes', '').strip()

        if not all([customer_name, customer_email, delivery_address]):
            messages.error(request, 'Veuillez remplir tous les champs obligatoires.')
            return render(request, 'orders/order_create.html', {'form': request.POST})

        # Récupère les items
        items = []
        i = 0
        while True:
            name     = request.POST.get(f'items[{i}][name]', '').strip()
            quantity = request.POST.get(f'items[{i}][quantity]', '')
            price    = request.POST.get(f'items[{i}][price]', '')
            if not name:
                break
            try:
                items.append({
                    'name':     name,
                    'quantity': int(quantity),
                    'price':    Decimal(price),
                })
            except (ValueError, Exception):
                pass
            i += 1

        if not items:
            messages.error(request, 'Ajoutez au moins un article.')
            return render(request, 'orders/order_create.html', {'form': request.POST})

        # Crée la commande en DB avec statut 'pending'
        order = Order.objects.create(
            customer_name=customer_name,
            customer_email=customer_email,
            delivery_address=delivery_address,
            notes=notes,
            status='pending',
        )

        total = Decimal('0')
        for item_data in items:
            OrderItem.objects.create(
                order=order,
                name=item_data['name'],
                quantity=item_data['quantity'],
                price=item_data['price'],
            )
            total += item_data['quantity'] * item_data['price']

        order.total_amount = total
        order.save()

        # ── Publie sur Kafka → order_details (comme producer.py) ──────────────
        kafka_ok = _publish_order_details(order)

        if kafka_ok:
            messages.success(
                request,
                f'✅ Commande #{order.id} créée et publiée sur Kafka (order_details). '
                f'Le consumer transaction.py va la traiter…'
            )
        else:
            messages.warning(
                request,
                f'⚠️ Commande #{order.id} créée mais Kafka indisponible. '
                f'Statut restera "pending".'
            )

        # Redirige vers le detail — le SSE mettra à jour le statut en live
        return redirect('orders:detail', pk=order.id)

    return render(request, 'orders/order_create.html', {'form': {}})


def order_detail(request, pk):
    order = get_object_or_404(Order.objects.prefetch_related('items'), pk=pk)
    payment = getattr(order, 'payment', None)
    return render(request, 'orders/order_detail.html', {
        'order':   order,
        'payment': payment,
    })


def order_status_api(request, pk):
    """
    API JSON légère pour le polling temps réel depuis order_detail.html.
    Retourne le statut actuel de la commande.
    """
    order = get_object_or_404(Order, pk=pk)
    payment = getattr(order, 'payment', None)
    return JsonResponse({
        'id':                order.id,
        'status':            order.status,
        'status_display':    order.get_status_display(),
        'confirmed_at':      order.confirmed_at.strftime('%H:%M:%S') if order.confirmed_at else None,
        'notification_sent': order.notification_sent,
        'payment_ref':       payment.reference if payment else None,
        'payment_method':    payment.get_method_display() if payment else None,
        'payment_status':    payment.status if payment else None,
    })

@csrf_exempt
def api_create_order(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'POST only'}, status=405)
    data = json.loads(request.body)
    order = Order.objects.create(
        customer_name=data.get('customer_name', data['user_id']),
        customer_email=data.get('customer_email', f"{data['user_id']}@gmail.com"),
        total_amount=data['total_cost'],
        delivery_address=data.get('delivery_address', '1 rue de la Paix'),
        status='pending',
    )
    # Ajouter les items
    for item_name in data.get('items', '').split(','):
        if item_name.strip():
            OrderItem.objects.create(order=order, name=item_name.strip(), price=0, quantity=1)
    return JsonResponse({'id': order.id})