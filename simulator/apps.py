from django.apps import AppConfig


class SimulatorConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "simulator"

    def ready(self):
        try:
            from .models import Config

            Config.get()  # populates all Redis keys from DB on every startup
        except Exception:
            # DB not available yet (first migrate, test runner, etc.)
            pass
