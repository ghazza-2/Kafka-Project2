# orders/api_views.py
"""
Endpoint REST minimal pour que le producer externe puisse créer
des Orders + OrderItems en DB avant de publier sur Kafka.

URL : POST /api/orders/create/

Body JSON :
{
    "customer_name":    "Ahmed Ben Ali",
    "customer_email":   "ahmed.benali@gmail.com",
    "delivery_address": "12 Av. Bourguiba, Tunis",
    "notes":            "",
    "items": [
        {"name": "Burger Classic", "quantity": 2, "price": 8.50},
        {"name": "Frites Maison",  "quantity": 1, "price": 3.50}
    ]
}

Réponse :
{
    "order_id": 42,
    "total_amount": "20.50",
    "status": "pending"
}
"""

import json
from decimal import Decimal

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import Order, OrderItem


@csrf_exempt
@require_http_methods(["POST"])
def create_order_api(request):
    try:
        data = json.loads(request.body)

        # ── Validation minimale ───────────────────────────────────────────────
        required = ["customer_name", "customer_email", "delivery_address", "items"]
        for field in required:
            if not data.get(field):
                return JsonResponse(
                    {"error": f"Champ manquant : {field}"},
                    status=400
                )

        items_data = data["items"]
        if not isinstance(items_data, list) or len(items_data) == 0:
            return JsonResponse({"error": "items doit être une liste non vide"}, status=400)

        # ── Calcul du total ───────────────────────────────────────────────────
        total = sum(
            Decimal(str(item["price"])) * int(item.get("quantity", 1))
            for item in items_data
        )

        # ── Création Order ────────────────────────────────────────────────────
        order = Order.objects.create(
            customer_name=data["customer_name"],
            customer_email=data["customer_email"],
            delivery_address=data["delivery_address"],
            notes=data.get("notes", ""),
            status="pending",
            total_amount=total,
        )

        # ── Création OrderItems ───────────────────────────────────────────────
        OrderItem.objects.bulk_create([
            OrderItem(
                order=order,
                name=item["name"],
                quantity=int(item.get("quantity", 1)),
                price=Decimal(str(item["price"])),
            )
            for item in items_data
        ])

        return JsonResponse({
            "order_id":     order.id,
            "total_amount": str(order.total_amount),
            "status":       order.status,
        }, status=201)

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return JsonResponse({"error": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"error": f"Erreur serveur : {e}"}, status=500)