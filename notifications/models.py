from django.db import models
from orders.models import Order


class Notification(models.Model):
    TYPE_CHOICES = [
        ('email', 'Email'),
        ('sms',   'SMS'),
        ('push',  'Push'),
    ]

    order     = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='notifications')
    type      = models.CharField(max_length=10, choices=TYPE_CHOICES, default='email')
    title     = models.CharField(max_length=200)
    message   = models.TextField()
    recipient = models.CharField(max_length=200)
    sent      = models.BooleanField(default=False)
    is_read   = models.BooleanField(default=False)
    sent_at   = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-sent_at']

    def __str__(self):
        return f"[{self.type.upper()}] {self.title} → {self.recipient}"