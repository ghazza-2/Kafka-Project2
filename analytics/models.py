from django.db import models


class OrderEvent(models.Model):
    """Enregistre chaque événement Kafka order_confirmed consommé."""
    order_id      = models.IntegerField()
    customer_name = models.CharField(max_length=200)
    total_amount  = models.DecimalField(max_digits=10, decimal_places=2)
    topic         = models.CharField(max_length=100, default='order_confirmed')
    received_at   = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-received_at']

    def __str__(self):
        return f"Event order#{self.order_id} at {self.received_at}"