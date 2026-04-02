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
    })


@csrf_exempt
@require_POST
def update_config(request):
    data = json.loads(request.body)
    config = Config.get()
    config.time_scale = max(1, int(data.get('time_scale', config.time_scale)))
    config.save()
    return JsonResponse({'time_scale': config.time_scale})


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
