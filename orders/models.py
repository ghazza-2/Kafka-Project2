from django.db import models


class Order(models.Model):
    STATUS_CHOICES = [
        ('pending',    'En attente'),
        ('processing', 'En traitement'),
        ('confirmed',  'Confirmé'),
        ('failed',     'Échoué'),
    ]

    customer_name     = models.CharField(max_length=200)
    customer_email    = models.EmailField()
    delivery_address  = models.TextField()
    notes             = models.TextField(blank=True)
    status            = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    total_amount      = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    created_at        = models.DateTimeField(auto_now_add=True)
    confirmed_at      = models.DateTimeField(null=True, blank=True)
    notification_sent = models.BooleanField(default=False)

    class Meta:
        ordering = ['-created_at']
        app_label = 'orders'

    def __str__(self):
        return f"Order #{self.id} — {self.customer_name}"

    def to_kafka_payload(self):
        return {
            'order_id':        self.id,
            'customer_name':   self.customer_name,
            'customer_email':  self.customer_email,
            'delivery_address': self.delivery_address,
            'total_amount':    str(self.total_amount),
            'items': [item.to_dict() for item in self.items.all()],
        }


class OrderItem(models.Model):
    order    = models.ForeignKey(Order, related_name='items', on_delete=models.CASCADE)
    name     = models.CharField(max_length=200)
    quantity = models.PositiveIntegerField(default=1)
    price    = models.DecimalField(max_digits=8, decimal_places=2)

    @property
    def subtotal(self):
        return self.quantity * self.price

    def to_dict(self):
        return {
            'name':     self.name,
            'quantity': self.quantity,
            'price':    str(self.price),
            'subtotal': str(self.subtotal),
        }

    def __str__(self):
        return f"{self.quantity}× {self.name}"