from django.urls import path

from . import views

app_name = 'simulator'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('control/', views.control, name='control'),
    path('config/', views.update_config, name='update_config'),
    path('noisy/', views.set_noisy, name='set_noisy'),
    path('spammer/', views.set_spammer, name='set_spammer'),
    path('bursty/', views.set_bursty, name='set_bursty'),
    path('normal/', views.set_normal, name='set_normal'),
    path('reset-modes/', views.reset_all_modes, name='reset_all_modes'),
    path('virtual-keys/', views.assign_virtual_keys, name='assign_virtual_keys'),
    path('virtual-keys/update/', views.update_virtual_keys, name='update_virtual_keys'),
    path('virtual-keys/clear/', views.clear_virtual_keys, name='clear_virtual_keys'),
    path('clear/', views.clear_stats, name='clear_stats'),
    path('strategies/', views.set_strategies, name='set_strategies'),
]
