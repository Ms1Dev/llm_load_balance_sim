from django.urls import path

from .consumers import SimulatorConsumer

websocket_urlpatterns = [
    path("ws/simulator/", SimulatorConsumer.as_asgi()),
]
