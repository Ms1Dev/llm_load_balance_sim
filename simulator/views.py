import json
import os

import redis as redis_sync
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import runner
from .models import Config

REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379')


def dashboard(request):
    config = Config.get()
    r = redis_sync.from_url(REDIS_URL)
    noisy_user_ids = [int(x) for x in r.smembers('config:noisy_users')]
    r.close()
    return render(request, 'simulator/dashboard.html', {
        "running": runner.is_running(),
        "rpm_limit": config.rpm_limit,
        "tpm_limit": config.tpm_limit,
        "noisy_user_ids": noisy_user_ids,
    })


@csrf_exempt
@require_POST
def update_config(request):
    data = json.loads(request.body)
    config = Config.get()
    if 'rpm_limit' in data:
        config.rpm_limit = max(1, int(data['rpm_limit']))
    if 'tpm_limit' in data:
        config.tpm_limit = max(1, int(data['tpm_limit']))
    config.save()
    return JsonResponse({
        'rpm_limit': config.rpm_limit,
        'tpm_limit': config.tpm_limit,
    })


@csrf_exempt
@require_POST
def set_noisy(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    r = redis_sync.from_url(REDIS_URL)
    r.delete('config:noisy_users')
    if user_ids:
        r.sadd('config:noisy_users', *user_ids)
    r.close()
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)("simulator", {
        "type": "simulator.event",
        "event_type": "noisy_users",
        "data": {"noisy_user_ids": user_ids},
    })
    return JsonResponse({'noisy_user_ids': user_ids})


@csrf_exempt
@require_POST
def control(request):
    data = json.loads(request.body)
    action = data.get("action")
    if action == "start":
        runner.start()
    elif action == "stop":
        runner.stop()
    return JsonResponse({"running": runner.is_running()})
