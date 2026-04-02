from django.urls import path
from . import views

app_name = 'simulator'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('control/', views.control, name='control'),
    path('events/', views.events, name='events'),
]
