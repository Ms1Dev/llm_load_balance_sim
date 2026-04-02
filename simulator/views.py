import json

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import runner


def dashboard(request):
    return render(request, 'simulator/dashboard.html', {
        "running": runner.is_running(),
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
