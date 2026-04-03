import os

import redis as redis_sync
from django.db import models

REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379')


class Config(models.Model):
    rpm_limit = models.PositiveIntegerField(
        default=200,
        help_text="Maximum requests per minute allowed by the limiter.",
    )
    tpm_limit = models.PositiveIntegerField(
        default=200000,
        help_text="Maximum tokens per minute allowed by the limiter.",
    )
    stats_window_minutes = models.PositiveIntegerField(
        default=5,
        help_text="Rolling window in minutes for per-user stats on the dashboard.",
    )

    class Meta:
        verbose_name = verbose_name_plural = "Configuration"

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        r.mset({
            'config:rpm_limit': self.rpm_limit,
            'config:tpm_limit': self.tpm_limit,
        })
        r.close()

    @classmethod
    def get(cls):
        obj, created = cls.objects.get_or_create(pk=1, defaults={
            'rpm_limit': 200,
            'tpm_limit': 200000,
            'stats_window_minutes': 5,
        })
        if not created:
            # Always push current values to Redis — save() only runs on create,
            # so a restart with an existing DB row would leave Redis stale.
            r = redis_sync.from_url(REDIS_URL)
            r.mset({
                'config:rpm_limit': obj.rpm_limit,
                'config:tpm_limit': obj.tpm_limit,
            })
            r.close()
        return obj
