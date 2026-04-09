import json
import time
from datetime import timedelta

from django.shortcuts import render
from django.http import StreamingHttpResponse
from django.utils import timezone
from django.db.models import Sum, Count, Avg
from django.db.models.functions import TruncDate
from django.views.decorators.http import condition

from orders.models import Order, OrderItem
from .models import OrderEvent


def dashboard(request):
    period = int(request.GET.get('period', 7))
    since  = timezone.now() - timedelta(days=period)

    # KPIs sur les commandes confirmées
    confirmed = Order.objects.filter(status='confirmed', confirmed_at__gte=since)
    prev_since = since - timedelta(days=period)
    prev_confirmed = Order.objects.filter(
        status='confirmed',
        confirmed_at__gte=prev_since,
        confirmed_at__lt=since,
    )

    total_orders   = confirmed.count()
    prev_orders    = prev_confirmed.count()
    revenue        = confirmed.aggregate(s=Sum('total_amount'))['s'] or 0
    prev_revenue   = prev_confirmed.aggregate(s=Sum('total_amount'))['s'] or 0
    avg_order      = confirmed.aggregate(a=Avg('total_amount'))['a'] or 0
    all_orders     = Order.objects.filter(created_at__gte=since).count()
    confirm_rate   = round(total_orders / all_orders * 100, 1) if all_orders else 0

    def trend(current, previous):
        if previous == 0:
            return '+0%'
        diff = ((current - previous) / previous) * 100
        sign = '+' if diff >= 0 else ''
        return f"{sign}{diff:.1f}%"

    kpis = {
        'total_orders':       total_orders,
        'orders_trend':       trend(total_orders, prev_orders),
        'revenue':            round(revenue, 2),
        'revenue_trend':      trend(float(revenue), float(prev_revenue)),
        'avg_order':          round(avg_order, 2),
        'confirmation_rate':  confirm_rate,
    }

    # Données graphiques : commandes et revenus par jour
    daily = (
        confirmed
        .annotate(day=TruncDate('confirmed_at'))
        .values('day')
        .annotate(orders=Count('id'), rev=Sum('total_amount'))
        .order_by('day')
    )

    all_days = [(since.date() + timedelta(days=i)) for i in range(period + 1)]
    daily_map = {row['day']: row for row in daily}

    chart_labels  = [d.strftime('%d/%m') for d in all_days]
    chart_orders  = [daily_map.get(d, {}).get('orders', 0) for d in all_days]
    chart_revenue = [float(daily_map.get(d, {}).get('rev') or 0) for d in all_days]

    # Top articles
    top_items_qs = (
        OrderItem.objects
        .filter(order__status='confirmed', order__confirmed_at__gte=since)
        .values('name')
        .annotate(count=Count('id'))
        .order_by('-count')[:8]
    )
    max_count = top_items_qs[0]['count'] if top_items_qs else 1
    top_items = [
        {**item, 'percentage': round(item['count'] / max_count * 100)}
        for item in top_items_qs
    ]

    # Activité Kafka récente (chargement initial)
    kafka_events = OrderEvent.objects.order_by('-received_at')[:20]

    # Commandes récentes (toutes)
    recent_orders = Order.objects.prefetch_related('items').order_by('-created_at')[:15]

    return render(request, 'analytics/dashboard.html', {
        'kpis':          kpis,
        'period':        str(period),
        'chart_labels':  json.dumps(chart_labels),
        'chart_orders':  json.dumps(chart_orders),
        'chart_revenue': json.dumps(chart_revenue),
        'top_items':     top_items,
        'kafka_events':  kafka_events,
        'recent_orders': recent_orders,
    })


def _build_snapshot():
    """Construit un snapshot JSON complet des données temps réel."""
    now = timezone.now()
    since_today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # KPIs
    confirmed_all  = Order.objects.filter(status='confirmed')
    confirmed_today = confirmed_all.filter(confirmed_at__gte=since_today)
    pending_count  = Order.objects.filter(status='pending').count()
    total_count    = Order.objects.count()
    revenue_total  = confirmed_all.aggregate(s=Sum('total_amount'))['s'] or 0
    revenue_today  = confirmed_today.aggregate(s=Sum('total_amount'))['s'] or 0
    avg_order      = confirmed_all.aggregate(a=Avg('total_amount'))['a'] or 0

    # Derniers événements Kafka
    events = list(
        OrderEvent.objects.order_by('-received_at')[:20].values(
            'order_id', 'customer_name', 'total_amount', 'topic', 'received_at'
        )
    )
    for e in events:
        e['received_at'] = e['received_at'].strftime('%H:%M:%S')
        e['total_amount'] = float(e['total_amount'])

    # Dernières commandes
    orders = []
    for o in Order.objects.prefetch_related('items').order_by('-created_at')[:15]:
        orders.append({
            'id':            o.id,
            'customer_name': o.customer_name,
            'customer_email': o.customer_email,
            'status':        o.status,
            'status_display': o.get_status_display(),
            'total_amount':  float(o.total_amount),
            'created_at':    o.created_at.strftime('%H:%M:%S'),
            'items_count':   o.items.count(),
            'item_names':    ', '.join(i.name for i in o.items.all()[:2]),
        })

    return {
        'kpis': {
            'total_orders':      confirmed_all.count(),
            'confirmed_today':   confirmed_today.count(),
            'pending':           pending_count,
            'total':             total_count,
            'revenue_total':     round(float(revenue_total), 2),
            'revenue_today':     round(float(revenue_today), 2),
            'avg_order':         round(float(avg_order), 2),
        },
        'events': events,
        'orders': orders,
        'ts':     now.strftime('%H:%M:%S'),
    }


def kafka_stream(request):
    """
    Server-Sent Events endpoint — envoie un snapshot JSON toutes les 2 secondes.
    Le frontend écoute ce flux et met à jour le DOM sans recharger la page.
    """
    def event_generator():
        last_event_id = None
        while True:
            try:
                snapshot = _build_snapshot()

                # Détecter un nouvel événement Kafka
                latest = OrderEvent.objects.order_by('-received_at').first()
                new_event = latest.id if latest else None

                if new_event != last_event_id:
                    last_event_id = new_event
                    snapshot['has_new'] = True
                else:
                    snapshot['has_new'] = False

                data = json.dumps(snapshot)
                yield f"data: {data}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

            time.sleep(2)

    response = StreamingHttpResponse(
        event_generator(),
        content_type='text/event-stream',
    )
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response