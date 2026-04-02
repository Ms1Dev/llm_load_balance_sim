import json

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import runner
from .models import Config


def dashboard(request):
    config = Config.get()
    return render(request, 'simulator/dashboard.html', {
        "running": runner.is_running(),
        "time_scale": config.time_scale,
        "rpm_limit": config.rpm_limit,
        "tpm_limit": config.tpm_limit,
    })


@csrf_exempt
@require_POST
def update_config(request):
    data = json.loads(request.body)
    config = Config.get()
    if 'time_scale' in data:
        config.time_scale = max(1, int(data['time_scale']))
    if 'rpm_limit' in data:
        config.rpm_limit = max(1, int(data['rpm_limit']))
    if 'tpm_limit' in data:
        config.tpm_limit = max(1, int(data['tpm_limit']))
    config.save()
    return JsonResponse({
        'time_scale': config.time_scale,
        'rpm_limit': config.rpm_limit,
        'tpm_limit': config.tpm_limit,
    })


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
