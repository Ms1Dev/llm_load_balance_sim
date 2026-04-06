import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis as redis_sync
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import runner
from .models import Config, SimUser, VirtualKeySettings

REDIS_URL          = os.environ.get('REDIS_URL',          'redis://redis:6379')
BIFROST_GOVERNANCE = os.environ.get('BIFROST_GOVERNANCE', 'http://bifrost:8080')
USER_IDS           = list(range(1, 101))

ALLOWED_STRATEGIES = {"backoff", "throttle"}


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


# ── Dashboard ─────────────────────────────────────────────────────────────

def dashboard(request):
    # Config.get() syncs all config + SimUser modes/vkeys → Redis
    config = Config.get()
    r = redis_sync.from_url(REDIS_URL)
    noisy_user_ids   = [int(x) for x in r.smembers('config:noisy_users')]
    spammer_user_ids = [int(x) for x in r.smembers('config:spammer_users')]
    bursty_user_ids  = [int(x) for x in r.zrangebyscore('config:bursty_users', time.time(), '+inf')]
    has_vkeys        = bool(r.exists('config:vkey:1'))
    active_strategies = [s.decode() for s in r.smembers('config:strategies')]
    bv = r.mget(
        'config:backoff:max_retries',
        'config:backoff:base_delay',
        'config:backoff:max_delay',
        'config:backoff:jitter',
    )
    r.close()
    return render(request, 'simulator/dashboard.html', {
        "running":        runner.is_running(),
        "rpm_limit":      config.rpm_limit,
        "tpm_limit":      config.tpm_limit,
        "noisy_user_ids":   noisy_user_ids,
        "spammer_user_ids": spammer_user_ids,
        "bursty_user_ids":  bursty_user_ids,
        "has_vkeys":        has_vkeys,
        "active_strategies_json": json.dumps(active_strategies),
        "backoff_max_retries": int(bv[0])   if bv[0] else config.backoff_max_retries,
        "backoff_base_delay":  float(bv[1]) if bv[1] else config.backoff_base_delay,
        "backoff_max_delay":   float(bv[2]) if bv[2] else config.backoff_max_delay,
        "backoff_jitter":      bv[3] != b'0' if bv[3] else config.backoff_jitter,
    })


# ── Config ────────────────────────────────────────────────────────────────

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
    return JsonResponse({'rpm_limit': config.rpm_limit, 'tpm_limit': config.tpm_limit})


@csrf_exempt
@require_POST
def set_backoff_config(request):
    data = json.loads(request.body)
    config = Config.get()
    if 'max_retries' in data:
        config.backoff_max_retries = max(0, int(data['max_retries']))
    if 'base_delay' in data:
        config.backoff_base_delay = max(0.1, float(data['base_delay']))
    if 'max_delay' in data:
        config.backoff_max_delay = max(0.1, float(data['max_delay']))
    if 'jitter' in data:
        config.backoff_jitter = bool(data['jitter'])
    config.save()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def set_strategies(request):
    data = json.loads(request.body)
    strategies = [s for s in data.get('strategies', []) if s in ALLOWED_STRATEGIES]
    config = Config.get()
    config.active_strategies = ','.join(strategies)
    config.save()   # save() syncs config:strategies to Redis
    async_to_sync(get_channel_layer().group_send)("simulator", {
        "type": "simulator.event",
        "event_type": "strategies",
        "data": {"active_strategies": strategies},
    })
    return JsonResponse({'strategies': strategies})


# ── Simulator control ─────────────────────────────────────────────────────

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
def clear_stats(request):
    runner.clear_stats()
    return JsonResponse({"ok": True})


# ── User modes ────────────────────────────────────────────────────────────

@csrf_exempt
@require_POST
def set_noisy(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    # Replace the entire noisy set: mark requested users noisy, demote any
    # previously-noisy users not in the new list back to normal.
    SimUser.objects.filter(id__in=user_ids).update(mode=SimUser.MODE_NOISY)
    SimUser.objects.filter(mode=SimUser.MODE_NOISY).exclude(id__in=user_ids).update(mode=SimUser.MODE_NORMAL)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def set_spammer(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    SimUser.objects.filter(id__in=user_ids).update(mode=SimUser.MODE_SPAMMER)
    SimUser.objects.filter(mode=SimUser.MODE_SPAMMER).exclude(id__in=user_ids).update(mode=SimUser.MODE_NORMAL)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def set_bursty(request):
    # Bursty is ephemeral (10s TTL) — Redis only, no DB write.
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get('user_ids', [])]
    r = redis_sync.from_url(REDIS_URL)
    if user_ids:
        r.srem('config:noisy_users',   *user_ids)
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
    if user_ids:
        SimUser.objects.filter(id__in=user_ids).update(mode=SimUser.MODE_NORMAL)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    if user_ids:
        r.zrem('config:bursty_users', *[str(uid) for uid in user_ids])
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_POST
def reset_all_modes(request):
    SimUser.objects.all().update(mode=SimUser.MODE_NORMAL)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    r.delete('config:bursty_users')   # ephemeral ZSET not covered by sync_all_to_redis
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({'ok': True})


# ── Virtual keys ──────────────────────────────────────────────────────────

def _bifrost_create_key(uid: int, rpm: int, tpm: int, budget: float, budget_reset: str) -> tuple[int, str | None, str | None]:
    payload = json.dumps({
        "name": f"Sim User {uid}",
        "rate_limit": {
            "token_max_limit": tpm,
            "token_reset_duration": "1m",
            "request_max_limit": rpm,
            "request_reset_duration": "1m",
        },
        "budget": {
            "max_limit": budget,
            "reset_duration": budget_reset,
        },
        "is_active": True,
    }).encode()
    req = urllib.request.Request(
        f"{BIFROST_GOVERNANCE}/api/governance/virtual-keys",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            vk = body["virtual_key"]
            return uid, vk["value"], vk["id"]
    except Exception as exc:
        return uid, None, str(exc)


@csrf_exempt
@require_POST
def assign_virtual_keys(request):
    data          = json.loads(request.body)
    rpm_per_user  = max(1, int(data.get('rpm_per_user', 50)))
    tpm_per_user  = max(1, int(data.get('tpm_per_user', 50_000)))
    budget_limit  = max(0.01, float(data.get('budget_limit', 1.0)))
    budget_reset  = data.get('budget_reset', '24h')

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(_bifrost_create_key, uid, rpm_per_user, tpm_per_user, budget_limit, budget_reset)
                   for uid in USER_IDS]
        results = [f.result() for f in as_completed(futures)]

    # Persist to DB
    created, failed = 0, []
    uid_to_user = {u.id: u for u in SimUser.objects.all()}
    to_update = []
    for uid, value, key_id in results:
        if value:
            u = uid_to_user[uid]
            u.vkey_value = value
            u.vkey_id    = key_id
            to_update.append(u)
            created += 1
        else:
            failed.append(uid)
    if to_update:
        SimUser.objects.bulk_update(to_update, ['vkey_value', 'vkey_id'])

    # Save global key settings
    vks = VirtualKeySettings.get()
    vks.rpm_per_user = rpm_per_user
    vks.tpm_per_user = tpm_per_user
    vks.budget_limit = budget_limit
    vks.budget_reset = budget_reset
    vks.save()

    # Sync Redis (bulk_update doesn't call save())
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    r.close()

    return JsonResponse({'created': created, 'failed': failed})


def _bifrost_update_key(key_id: str, rpm: int, tpm: int, budget: float, budget_reset: str) -> tuple[str, bool]:
    payload = json.dumps({
        "rate_limit": {
            "token_max_limit": tpm,
            "token_reset_duration": "1m",
            "request_max_limit": rpm,
            "request_reset_duration": "1m",
        },
        "budget": {
            "max_limit": budget,
            "reset_duration": budget_reset,
        },
    }).encode()
    req = urllib.request.Request(
        f"{BIFROST_GOVERNANCE}/api/governance/virtual-keys/{key_id}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            return key_id, True
    except Exception:
        return key_id, False


@csrf_exempt
@require_POST
def update_virtual_keys(request):
    data          = json.loads(request.body)
    rpm_per_user  = max(1, int(data.get('rpm_per_user', 50)))
    tpm_per_user  = max(1, int(data.get('tpm_per_user', 50_000)))
    budget_limit  = max(0.01, float(data.get('budget_limit', 1.0)))
    budget_reset  = data.get('budget_reset', '24h')

    # Read key IDs from DB
    tasks = [(u.vkey_id, u.id) for u in SimUser.objects.exclude(vkey_id='')]
    if not tasks:
        return JsonResponse({'updated': 0, 'failed': [], 'error': 'no keys assigned'}, status=400)

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(_bifrost_update_key, key_id, rpm_per_user, tpm_per_user, budget_limit, budget_reset)
                   for key_id, _ in tasks]
        results = [f.result() for f in as_completed(futures)]

    # Persist updated settings
    vks = VirtualKeySettings.get()
    vks.rpm_per_user = rpm_per_user
    vks.tpm_per_user = tpm_per_user
    vks.budget_limit = budget_limit
    vks.budget_reset = budget_reset
    vks.save()

    updated = sum(1 for _, ok in results if ok)
    failed  = [kid for kid, ok in results if not ok]
    return JsonResponse({'updated': updated, 'failed': failed})


@csrf_exempt
@require_POST
def clear_virtual_keys(request):
    SimUser.objects.all().update(vkey_value='', vkey_id='')
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)   # clears vkey:* keys since values are now empty
    r.close()
    return JsonResponse({'ok': True})
