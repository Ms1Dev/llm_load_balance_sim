import json
import os
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

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
BIFROST_GOVERNANCE = os.environ.get("BIFROST_GOVERNANCE", "http://bifrost:8080")
USER_IDS = list(range(1, 101))
COST_PER_1M_INPUT = float(os.environ.get("COST_PER_1M_INPUT", "0.75"))
COST_PER_1M_OUTPUT = float(os.environ.get("COST_PER_1M_OUTPUT", "4.50"))

ALLOWED_STRATEGIES = {"backoff", "throttle"}


def _broadcast_user_modes(r):
    """Broadcast the current bursty/spammer/pro sets over WebSocket."""
    bursty_ids = [int(x) for x in r.smembers("config:bursty_users")]
    spammer_ids = [int(x) for x in r.smembers("config:spammer_users")]
    pro_ids = [int(x) for x in r.smembers("config:pro_users")]
    async_to_sync(get_channel_layer().group_send)(
        "simulator",
        {
            "type": "simulator.event",
            "event_type": "user_modes",
            "data": {"bursty": bursty_ids, "spammer": spammer_ids, "pro": pro_ids},
        },
    )


# ── Dashboard ─────────────────────────────────────────────────────────────

SIM_USER_IDS = list(range(1, 51))  # users actually simulated by runner.py


def _split_reset_duration(
    duration: str, default_n: int, default_unit: str
) -> tuple[int, str]:
    raw = str(duration or "").strip()
    if len(raw) < 2:
        return default_n, default_unit
    unit = raw[-1]
    if unit not in {"m", "h", "d", "w", "M", "Y"}:
        return default_n, default_unit
    try:
        n = int(raw[:-1])
    except ValueError:
        return default_n, default_unit
    return max(1, n), unit


def dashboard(request):
    # Config.get() syncs all config + SimUser modes/vkeys → Redis
    config = Config.get()
    r = redis_sync.from_url(REDIS_URL)
    bursty_user_ids = [int(x) for x in r.smembers("config:bursty_users")]
    spammer_user_ids = [int(x) for x in r.smembers("config:spammer_users")]
    pro_user_ids = [int(x) for x in r.smembers("config:pro_users")]
    has_vkeys = bool(r.exists("config:vkey:1"))
    active_strategies = [s.decode() for s in r.smembers("config:strategies")]
    # Read spend from Redis
    pipe = r.pipeline()
    for uid in SIM_USER_IDS:
        pipe.hget(f"usage:{uid}:cost", "total")
    redis_spend = {uid: float(v or 0) for uid, v in zip(SIM_USER_IDS, pipe.execute())}
    r.close()

    # Merge with DB (max wins): Redis is live data; DB survives Redis restarts
    db_users = {u.id: u for u in SimUser.objects.filter(id__in=SIM_USER_IDS)}
    user_spend = {}
    to_update = []
    for uid in SIM_USER_IDS:
        u = db_users.get(uid)
        db_val = u.spend if u else 0.0
        merged = max(redis_spend.get(uid, 0.0), db_val)
        user_spend[uid] = merged
        if u and merged > db_val:
            u.spend = merged
            to_update.append(u)
    if to_update:
        SimUser.objects.bulk_update(to_update, ["spend"])
    vks = VirtualKeySettings.get()
    basic_requests = max(1, int(getattr(vks, "requests_per_user", 10) or 10))
    basic_tokens = max(1, int(getattr(vks, "tokens_per_user", 10000) or 10000))
    basic_budget = max(0.01, float(getattr(vks, "budget_limit", 1.0) or 1.0))
    basic_requests_reset_n, basic_requests_reset_unit = _split_reset_duration(
        getattr(vks, "requests_reset", "1m") or "1m", 1, "m"
    )
    basic_tokens_reset_n, basic_tokens_reset_unit = _split_reset_duration(
        getattr(vks, "tokens_reset", "1m") or "1m", 1, "m"
    )
    basic_budget_reset_n, basic_budget_reset_unit = _split_reset_duration(
        getattr(vks, "budget_reset", "24h") or "24h", 24, "h"
    )
    pro_requests = max(1, int(getattr(vks, "pro_requests_per_user", 20) or 20))
    pro_tokens = max(1, int(getattr(vks, "pro_tokens_per_user", 20000) or 20000))
    pro_budget = max(0.01, float(getattr(vks, "pro_budget_limit", 5.0) or 5.0))
    pro_requests_reset_n, pro_requests_reset_unit = _split_reset_duration(
        getattr(vks, "pro_requests_reset", "1m") or "1m", 1, "m"
    )
    pro_tokens_reset_n, pro_tokens_reset_unit = _split_reset_duration(
        getattr(vks, "pro_tokens_reset", "1m") or "1m", 1, "m"
    )
    pro_budget_reset_n, pro_budget_reset_unit = _split_reset_duration(
        getattr(vks, "pro_budget_reset", "24h") or "24h", 24, "h"
    )

    return render(
        request,
        "simulator/dashboard.html",
        {
            "running": runner.is_running(),
            "rpm_limit": config.rpm_limit,
            "tpm_limit": config.tpm_limit,
            "normal_user_rpm": config.normal_user_rpm,
            "bursty_user_ids": bursty_user_ids,
            "spammer_user_ids": spammer_user_ids,
            "pro_user_ids": pro_user_ids,
            "has_vkeys": has_vkeys,
            "active_strategies_json": json.dumps(active_strategies),
            "cost_per_1m_input": COST_PER_1M_INPUT,
            "cost_per_1m_output": COST_PER_1M_OUTPUT,
            "user_spend_json": json.dumps(user_spend),
            "usage_pattern": config.usage_pattern,
            "vk_basic_requests": basic_requests,
            "vk_basic_tokens": basic_tokens,
            "vk_basic_budget": basic_budget,
            "vk_basic_requests_reset_n": basic_requests_reset_n,
            "vk_basic_requests_reset_unit": basic_requests_reset_unit,
            "vk_basic_tokens_reset_n": basic_tokens_reset_n,
            "vk_basic_tokens_reset_unit": basic_tokens_reset_unit,
            "vk_basic_budget_reset_n": basic_budget_reset_n,
            "vk_basic_budget_reset_unit": basic_budget_reset_unit,
            "vk_pro_requests": pro_requests,
            "vk_pro_tokens": pro_tokens,
            "vk_pro_budget": pro_budget,
            "vk_pro_requests_reset_n": pro_requests_reset_n,
            "vk_pro_requests_reset_unit": pro_requests_reset_unit,
            "vk_pro_tokens_reset_n": pro_tokens_reset_n,
            "vk_pro_tokens_reset_unit": pro_tokens_reset_unit,
            "vk_pro_budget_reset_n": pro_budget_reset_n,
            "vk_pro_budget_reset_unit": pro_budget_reset_unit,
        },
    )


# ── Config ────────────────────────────────────────────────────────────────


@csrf_exempt
@require_POST
def update_config(request):
    data = json.loads(request.body)
    config = Config.get()
    if "rpm_limit" in data:
        config.rpm_limit = max(1, int(data["rpm_limit"]))
    if "tpm_limit" in data:
        config.tpm_limit = max(1, int(data["tpm_limit"]))
    if "normal_user_rpm" in data:
        config.normal_user_rpm = max(1, int(data["normal_user_rpm"]))
    config.save()
    return JsonResponse(
        {
            "rpm_limit": config.rpm_limit,
            "tpm_limit": config.tpm_limit,
            "normal_user_rpm": config.normal_user_rpm,
        }
    )


ALLOWED_USAGE_PATTERNS = {"consistent", "sine_wave", "bursty"}


@csrf_exempt
@require_POST
def set_usage_pattern(request):
    data = json.loads(request.body)
    pattern = data.get("pattern", "sine_wave")
    if pattern not in ALLOWED_USAGE_PATTERNS:
        return JsonResponse({"error": "invalid pattern"}, status=400)
    config = Config.get()
    config.usage_pattern = pattern
    config.save()  # save() syncs config:usage_pattern → Redis
    return JsonResponse({"pattern": pattern})


@csrf_exempt
@require_POST
def set_strategies(request):
    data = json.loads(request.body)
    strategies = [s for s in data.get("strategies", []) if s in ALLOWED_STRATEGIES]
    config = Config.get()
    config.active_strategies = ",".join(strategies)
    config.save()  # save() syncs config:strategies to Redis
    async_to_sync(get_channel_layer().group_send)(
        "simulator",
        {
            "type": "simulator.event",
            "event_type": "strategies",
            "data": {"active_strategies": strategies},
        },
    )
    return JsonResponse({"strategies": strategies})


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
    SimUser.objects.filter(id__in=SIM_USER_IDS).update(spend=0.0)
    r = redis_sync.from_url(REDIS_URL)
    pipe = r.pipeline()
    for uid in SIM_USER_IDS:
        pipe.delete(f"usage:{uid}:cost", f"usage:{uid}:tokens")
    pipe.delete("usage:global:cost")
    pipe.execute()
    r.close()
    return JsonResponse({"ok": True})


# ── User modes ────────────────────────────────────────────────────────────


@csrf_exempt
@require_POST
def set_spammer(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get("user_ids", [])]
    SimUser.objects.filter(id__in=user_ids).update(mode=SimUser.MODE_SPAMMER)
    SimUser.objects.filter(mode=SimUser.MODE_SPAMMER).exclude(id__in=user_ids).update(
        mode=SimUser.MODE_NORMAL
    )
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def set_bursty(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get("user_ids", [])]
    SimUser.objects.filter(id__in=user_ids).update(mode=SimUser.MODE_BURSTY)
    SimUser.objects.filter(mode=SimUser.MODE_BURSTY).exclude(id__in=user_ids).update(
        mode=SimUser.MODE_NORMAL
    )
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def set_normal(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get("user_ids", [])]
    if user_ids:
        SimUser.objects.filter(id__in=user_ids).update(mode=SimUser.MODE_NORMAL)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def reset_all_modes(request):
    SimUser.objects.all().update(mode=SimUser.MODE_NORMAL)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def set_tier(request):
    data = json.loads(request.body)
    user_ids = [int(i) for i in data.get("user_ids", [])]
    tier = data.get("tier", SimUser.TIER_BASIC)
    if tier not in (SimUser.TIER_BASIC, SimUser.TIER_PRO):
        return JsonResponse({"error": "invalid tier"}, status=400)
    if user_ids:
        SimUser.objects.filter(id__in=user_ids).update(tier=tier)
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    _broadcast_user_modes(r)
    r.close()

    # If virtual keys are active, update the affected users' keys to their new tier's limits
    affected = list(SimUser.objects.filter(id__in=user_ids).exclude(vkey_id=""))
    if affected:
        vks = VirtualKeySettings.get()
        if tier == SimUser.TIER_PRO:
            requests = max(
                1,
                int(
                    data.get(
                        "pro_requests", data.get("pro_rpm", vks.pro_requests_per_user)
                    )
                ),
            )
            request_reset = data.get("pro_requests_reset", vks.pro_requests_reset)
            tokens = max(
                1,
                int(
                    data.get("pro_tokens", data.get("pro_tpm", vks.pro_tokens_per_user))
                ),
            )
            tokens_reset = data.get("pro_tokens_reset", vks.pro_tokens_reset)
            budget = max(0.01, float(data.get("pro_budget", vks.pro_budget_limit)))
            budget_reset = data.get("pro_budget_reset", vks.pro_budget_reset)
        else:
            requests = max(
                1,
                int(
                    data.get(
                        "basic_requests", data.get("basic_rpm", vks.requests_per_user)
                    )
                ),
            )
            request_reset = data.get("basic_requests_reset", vks.requests_reset)
            tokens = max(
                1,
                int(
                    data.get("basic_tokens", data.get("basic_tpm", vks.tokens_per_user))
                ),
            )
            tokens_reset = data.get("basic_tokens_reset", vks.tokens_reset)
            budget = max(0.01, float(data.get("basic_budget", vks.budget_limit)))
            budget_reset = data.get("basic_budget_reset", vks.budget_reset)
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = [
                ex.submit(
                    _bifrost_update_key,
                    u.vkey_id,
                    requests,
                    request_reset,
                    tokens,
                    tokens_reset,
                    budget,
                    budget_reset,
                    _key_name(u.id, tier),
                )
                for u in affected
            ]
            [f.result() for f in as_completed(futures)]

    return JsonResponse({"ok": True})


# ── Virtual keys ──────────────────────────────────────────────────────────


def _key_name(uid: int, tier: str) -> str:
    return f"User {uid} [{'Pro' if tier == SimUser.TIER_PRO else 'Basic'}]"


def _bifrost_create_key(
    uid: int,
    requests: int,
    requests_reset: str,
    tokens: int,
    tokens_reset: str,
    budget: float,
    budget_reset: str,
    name: str | None = None,
) -> tuple[int, str | None, str | None]:
    payload = json.dumps(
        {
            "name": name or f"User {uid}",
            "rate_limit": {
                "token_max_limit": tokens,
                "token_reset_duration": tokens_reset,
                "request_max_limit": requests,
                "request_reset_duration": requests_reset,
            },
            "budget": {
                "max_limit": budget,
                "reset_duration": budget_reset,
            },
            "is_active": True,
        }
    ).encode()
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
    # Always start fresh: delete existing keys from Bifrost and clear DB
    _delete_all_bifrost_keys()
    SimUser.objects.filter(id__in=SIM_USER_IDS).update(vkey_value="", vkey_id="")

    data = json.loads(request.body)
    basic_requests = max(1, int(data.get("basic_requests", data.get("basic_rpm", 10))))
    basic_requests_reset = data.get("basic_requests_reset", "1m")
    basic_tokens = max(1, int(data.get("basic_tokens", data.get("basic_tpm", 10_000))))
    basic_tokens_reset = data.get("basic_tokens_reset", "1m")
    basic_budget = max(0.01, float(data.get("basic_budget", 1.0)))
    basic_budget_reset = data.get("basic_budget_reset", "24h")
    pro_requests = max(1, int(data.get("pro_requests", data.get("pro_rpm", 20))))
    pro_requests_reset = data.get("pro_requests_reset", "1m")
    pro_tokens = max(1, int(data.get("pro_tokens", data.get("pro_tpm", 20_000))))
    pro_tokens_reset = data.get("pro_tokens_reset", "1m")
    pro_budget = max(0.01, float(data.get("pro_budget", 5.0)))
    pro_budget_reset = data.get("pro_budget_reset", "24h")

    users = list(SimUser.objects.filter(id__in=SIM_USER_IDS))
    pro_ids = {u.id for u in users if u.tier == SimUser.TIER_PRO}

    def _params(uid):
        is_pro = uid in pro_ids
        limits = (
            (
                pro_requests,
                pro_requests_reset,
                pro_tokens,
                pro_tokens_reset,
                pro_budget,
                pro_budget_reset,
            )
            if is_pro
            else (
                basic_requests,
                basic_requests_reset,
                basic_tokens,
                basic_tokens_reset,
                basic_budget,
                basic_budget_reset,
            )
        )
        return (
            *limits,
            _key_name(uid, SimUser.TIER_PRO if is_pro else SimUser.TIER_BASIC),
        )

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [ex.submit(_bifrost_create_key, u.id, *_params(u.id)) for u in users]
        results = [f.result() for f in as_completed(futures)]

    created, failed = 0, []
    uid_to_user = {u.id: u for u in users}
    to_update = []
    for uid, value, key_id in results:
        if value:
            u = uid_to_user[uid]
            u.vkey_value = value
            u.vkey_id = key_id
            to_update.append(u)
            created += 1
        else:
            failed.append(uid)
    if to_update:
        SimUser.objects.bulk_update(to_update, ["vkey_value", "vkey_id"])

    vks = VirtualKeySettings.get()
    vks.requests_per_user = basic_requests
    vks.requests_reset = basic_requests_reset
    vks.tokens_per_user = basic_tokens
    vks.tokens_reset = basic_tokens_reset
    vks.budget_limit = basic_budget
    vks.budget_reset = basic_budget_reset
    vks.pro_requests_per_user = pro_requests
    vks.pro_requests_reset = pro_requests_reset
    vks.pro_tokens_per_user = pro_tokens
    vks.pro_tokens_reset = pro_tokens_reset
    vks.pro_budget_limit = pro_budget
    vks.pro_budget_reset = pro_budget_reset
    vks.save()

    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    r.close()

    return JsonResponse({"created": created, "failed": failed})


def _bifrost_update_key(
    key_id: str,
    requests: int,
    request_reset: str,
    tokens: int,
    tokens_reset: str,
    budget: float,
    budget_reset: str,
    name: str | None = None,
) -> tuple[str, bool]:
    body: dict = {
        "rate_limit": {
            "token_max_limit": tokens,
            "token_reset_duration": tokens_reset,
            "request_max_limit": requests,
            "request_reset_duration": request_reset,
        },
        "budget": {
            "max_limit": budget,
            "reset_duration": budget_reset,
        },
    }

    if name:
        body["name"] = name
    payload = json.dumps(body).encode()
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
    data = json.loads(request.body)
    basic_requests = max(1, int(data.get("basic_requests", 10)))
    basic_tokens = max(1, int(data.get("basic_tokens", 10_000)))
    basic_requests_reset = data.get("basic_requests_reset", "1m")
    basic_tokens_reset = data.get("basic_tokens_reset", "1m")
    basic_budget = max(0.01, float(data.get("basic_budget", 1.0)))
    basic_budget_reset = data.get("basic_budget_reset", "24h")
    pro_requests = max(1, int(data.get("pro_requests", 20)))
    pro_tokens = max(1, int(data.get("pro_tokens", 20_000)))
    pro_requests_reset = data.get("pro_requests_reset", "1m")
    pro_tokens_reset = data.get("pro_tokens_reset", "1m")
    pro_budget = max(0.01, float(data.get("pro_budget", 5.0)))
    pro_budget_reset = data.get("pro_budget_reset", "24h")

    users = list(SimUser.objects.exclude(vkey_id=""))
    if not users:
        return JsonResponse(
            {"updated": 0, "failed": [], "error": "no keys assigned"}, status=400
        )

    pro_ids = {u.id for u in users if u.tier == SimUser.TIER_PRO}

    def _params(u):
        is_pro = u.id in pro_ids
        limits = (
            (
                pro_requests,
                pro_requests_reset,
                pro_tokens,
                pro_tokens_reset,
                pro_budget,
                pro_budget_reset,
            )
            if is_pro
            else (
                basic_requests,
                basic_requests_reset,
                basic_tokens,
                basic_tokens_reset,
                basic_budget,
                basic_budget_reset,
            )
        )
        return (
            *limits,
            _key_name(u.id, SimUser.TIER_PRO if is_pro else SimUser.TIER_BASIC),
        )

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = [
            ex.submit(_bifrost_update_key, u.vkey_id, *_params(u)) for u in users
        ]
        results = [f.result() for f in as_completed(futures)]

    vks = VirtualKeySettings.get()
    vks.requests_per_user = basic_requests
    vks.requests_reset = basic_requests_reset
    vks.tokens_per_user = basic_tokens
    vks.tokens_reset = basic_tokens_reset
    vks.budget_limit = basic_budget
    vks.budget_reset = basic_budget_reset
    vks.pro_requests_per_user = pro_requests
    vks.pro_requests_reset = pro_requests_reset
    vks.pro_tokens_per_user = pro_tokens
    vks.pro_tokens_reset = pro_tokens_reset
    vks.pro_budget_limit = pro_budget
    vks.pro_budget_reset = pro_budget_reset
    vks.save()

    updated = sum(1 for _, ok in results if ok)
    failed = [kid for kid, ok in results if not ok]
    return JsonResponse({"updated": updated, "failed": failed})


def _bifrost_delete_key(key_id: str) -> bool:
    req = urllib.request.Request(
        f"{BIFROST_GOVERNANCE}/api/governance/virtual-keys/{key_id}",
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            return True
    except Exception:
        return False


def _bifrost_list_sim_key_ids() -> list[str]:
    """Return IDs of all virtual keys in Bifrost named 'Sim User <n>'."""
    req = urllib.request.Request(
        f"{BIFROST_GOVERNANCE}/api/governance/virtual-keys?limit=500",
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            keys = body.get("virtual_keys") or body.get("keys") or []

            def _is_sim_key(name: str) -> bool:
                return name.startswith("Sim User ") or (
                    name.startswith("User ") and ("[Basic]" in name or "[Pro]" in name)
                )

            return [k["id"] for k in keys if _is_sim_key(str(k.get("name", "")))]
    except Exception:
        return []


def _delete_all_bifrost_keys():
    """Delete all Sim User keys from Bifrost, regardless of what is recorded in our DB."""
    key_ids = _bifrost_list_sim_key_ids()
    if key_ids:
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = [ex.submit(_bifrost_delete_key, kid) for kid in key_ids]
            [f.result() for f in as_completed(futures)]


@csrf_exempt
@require_POST
def clear_virtual_keys(request):
    _delete_all_bifrost_keys()
    SimUser.objects.filter(id__in=SIM_USER_IDS).update(vkey_value="", vkey_id="")
    r = redis_sync.from_url(REDIS_URL)
    SimUser.sync_all_to_redis(r)
    r.close()
    return JsonResponse({"ok": True})
