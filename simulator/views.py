import json
import os
import time

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


def _broadcast_user_modes(r):
    """Broadcast the current noisy/spammer/bursty sets over WebSocket."""
    noisy_ids   = [int(x) for x in r.smembers('config:noisy_users')]
    spammer_ids = [int(x) for x in r.smembers('config:spammer_users')]
    bursty_ids  = [int(x) for x in r.zrangebyscore('config:bursty_users', time.time(), '+inf')]
    async_to_sync(get_channel_layer().group_send)("simulator", {
        "type": "simulator.event",
        "event_type": "user_modes",
        "data": {"noisy": noisy_ids, "spammer": spammer_ids, "bursty": bursty_ids},
    })


ALLOWED_STRATEGIES = {"backoff", "throttle"}


def dashboard(request):
    config = Config.get()
    r = redis_sync.from_url(REDIS_URL)
    noisy_user_ids   = [int(x) for x in r.smembers('config:noisy_users')]
    spammer_user_ids = [int(x) for x in r.smembers('config:spammer_users')]
    bursty_user_ids  = [int(x) for x in r.zrangebyscore('config:bursty_users', time.time(), '+inf')]
    active_strategies = [s.decode() for s in r.smembers('config:strategies')]
    bv = r.mget(
        'config:backoff:max_retries',
        'config:backoff:base_delay',
        'config:backoff:max_delay',
        'config:backoff:jitter',
    )
    r.close()
    return render(request, 'simulator/dashboard.html', {
        "running": runner.is_running(),
        "rpm_limit": config.rpm_limit,
        "tpm_limit": config.tpm_limit,
        "noisy_user_ids":   noisy_user_ids,
        "spammer_user_ids": spammer_user_ids,
        "bursty_user_ids":  bursty_user_ids,
        "active_strategies_json": json.dumps(active_strategies),
        "backoff_max_retries": int(bv[0])   if bv[0] else 5,
        "backoff_base_delay":  float(bv[1]) if bv[1] else 1.0,
        "backoff_max_delay":   float(bv[2]) if bv[2] else 60.0,
        "backoff_jitter":      bv[3] != b'0' if bv[3] else True,
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
        r.srem('config:spammer_users', *user_ids)
        r.zrem('config:bursty_users', *[str(uid) for uid in user_ids])
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


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


@csrf_exempt
@require_POST
def set_strategies(request):
    data = json.loads(request.body)
    strategies = [s for s in data.get('strategies', []) if s in ALLOWED_STRATEGIES]
    r = redis_sync.from_url(REDIS_URL)
    r.delete('config:strategies')
    if strategies:
        r.sadd('config:strategies', *strategies)
    r.close()
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)("simulator", {
        "type": "simulator.event",
        "event_type": "strategies",
        "data": {"active_strategies": strategies},
    })
    return JsonResponse({'strategies': strategies})


@csrf_exempt
@require_POST
def set_backoff_config(request):
    data = json.loads(request.body)
    r = redis_sync.from_url(REDIS_URL)
    updates = {}
    if 'max_retries' in data:
        updates['config:backoff:max_retries'] = max(0, int(data['max_retries']))
    if 'base_delay' in data:
        updates['config:backoff:base_delay'] = max(0.1, float(data['base_delay']))
    if 'max_delay' in data:
        updates['config:backoff:max_delay'] = max(0.1, float(data['max_delay']))
    if 'jitter' in data:
        updates['config:backoff:jitter'] = '1' if data['jitter'] else '0'
    if updates:
        r.mset(updates)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def clear_stats(request):
    runner.clear_stats()
    return JsonResponse({"ok": True})

@csrf_exempt
@require_POST
def set_spammer(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    r = redis_sync.from_url(REDIS_URL)
    r.delete('config:spammer_users')
    if user_ids:
        r.sadd('config:spammer_users', *user_ids)
        r.srem('config:noisy_users', *user_ids)
        r.zrem('config:bursty_users', *[str(uid) for uid in user_ids])
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def set_bursty(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    r = redis_sync.from_url(REDIS_URL)
    if user_ids:
        r.srem('config:noisy_users', *user_ids)
        r.srem('config:spammer_users', *user_ids)
        expiry = time.time() + 10
        r.zadd('config:bursty_users', {str(uid): expiry for uid in user_ids})
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def set_normal(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    r = redis_sync.from_url(REDIS_URL)
    if user_ids:
        r.srem('config:noisy_users', *user_ids)
        r.srem('config:spammer_users', *user_ids)
        r.zrem('config:bursty_users', *[str(uid) for uid in user_ids])
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def reset_all_modes(request):
    r = redis_sync.from_url(REDIS_URL)
    r.delete('config:noisy_users', 'config:spammer_users', 'config:bursty_users')
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})