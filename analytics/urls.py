from django.urls import path
from . import views

app_name = 'analytics'

urlpatterns = [
    path('',        views.dashboard,    name='dashboard'),
    path('stream/', views.kafka_stream, name='stream'),
]