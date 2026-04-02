import os

import redis as redis_sync
from django.db import models

REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379')


class Config(models.Model):
    time_scale = models.PositiveIntegerField(
        default=4,
        help_text="Compresses the rate-limit window. 4 = 60s window behaves like 15s.",
    )

    class Meta:
        verbose_name = verbose_name_plural = "Configuration"

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        r.set('config:time_scale', self.time_scale)
        r.close()

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(pk=1, defaults={'time_scale': 4})
        return obj
