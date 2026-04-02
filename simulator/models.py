import os

import redis as redis_sync
from django.db import models

REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379')


class Config(models.Model):
    time_scale = models.PositiveIntegerField(
        default=4,
        help_text="Compresses the rate-limit window. 4 = 60s window behaves like 15s.",
    )
    rpm_limit = models.PositiveIntegerField(
        default=30,
        help_text="Maximum requests per minute allowed by the limiter.",
    )
    tpm_limit = models.PositiveIntegerField(
        default=10000,
        help_text="Maximum tokens per minute allowed by the limiter.",
    )

    class Meta:
        verbose_name = verbose_name_plural = "Configuration"

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        r.mset({
            'config:time_scale': self.time_scale,
            'config:rpm_limit': self.rpm_limit,
            'config:tpm_limit': self.tpm_limit,
        })
        r.close()

    @classmethod
    def get(cls):
        obj, created = cls.objects.get_or_create(pk=1, defaults={
            'time_scale': 4,
            'rpm_limit': 30,
            'tpm_limit': 10000,
        })
        if not created:
            # Always push current values to Redis — save() only runs on create,
            # so a restart with an existing DB row would leave Redis stale.
            r = redis_sync.from_url(REDIS_URL)
            r.mset({
                'config:time_scale': obj.time_scale,
                'config:rpm_limit': obj.rpm_limit,
                'config:tpm_limit': obj.tpm_limit,
            })
            r.close()
        return obj
