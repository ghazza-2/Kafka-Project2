from django.urls import path
from . import views

app_name = 'payments'

urlpatterns = [
    path('',                         views.payment_list,    name='list'),
    path('process/<int:order_id>/',  views.process_payment, name='process'),
]