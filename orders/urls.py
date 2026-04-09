from django.urls import path
from . import views


app_name = 'orders'

urlpatterns = [
    path('',                   views.order_list,       name='list'),
    path('home/',              views.order_list,       name='home'),
    path('new/',               views.order_create,     name='create'),
    path('<int:pk>/',          views.order_detail,     name='detail'),
    path('<int:pk>/status/',   views.order_status_api, name='status_api'),
    path('api/create/', views.api_create_order, name='api_create'),
]