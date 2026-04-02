from django.contrib import admin
from django.urls import path, include
from accounts import views as account_views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('accounts/', include('allauth.urls')),
    path('accounts/plan/', account_views.update_plan, name='update-plan'),
    path('', include('chat.urls')),
    path('test-runner/', include('test_runner.urls')),
]
