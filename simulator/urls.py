from django.urls import path
from . import views

app_name = 'simulator'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
]
