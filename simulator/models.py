import os

import redis as redis_sync
from django.db import models

REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379')


class Config(models.Model):
    rpm_limit = models.PositiveIntegerField(
        default=200,
        help_text="Maximum requests per minute allowed by the vLLM.",
    )
    tpm_limit = models.PositiveIntegerField(
        default=200000,
        help_text="Maximum tokens per minute allowed by the vLLM.",
    )
    stats_window_minutes = models.PositiveIntegerField(
        default=1,
        help_text="Legacy field; user tiles use a fixed 60s window for RPM and avg latency.",
    )
    backoff_max_retries = models.PositiveIntegerField(default=5)
    backoff_base_delay  = models.FloatField(default=1.0)
    backoff_max_delay   = models.FloatField(default=60.0)
    backoff_jitter      = models.BooleanField(default=True)
    # Comma-separated active strategy names, e.g. 'backoff,throttle'
    active_strategies   = models.CharField(max_length=64, blank=True, default='')

    class Meta:
        verbose_name = verbose_name_plural = "Configuration"

    def _sync_to_redis(self, r):
        pipe = r.pipeline()
        pipe.mset({
            'config:rpm_limit':           self.rpm_limit,
            'config:tpm_limit':           self.tpm_limit,
            'config:backoff:max_retries': self.backoff_max_retries,
            'config:backoff:base_delay':  self.backoff_base_delay,
            'config:backoff:max_delay':   self.backoff_max_delay,
            'config:backoff:jitter':      '1' if self.backoff_jitter else '0',
        })
        strategies = [s for s in self.active_strategies.split(',') if s]
        pipe.delete('config:strategies')
        if strategies:
            pipe.sadd('config:strategies', *strategies)
        pipe.execute()

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        self._sync_to_redis(r)
        r.close()

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(pk=1, defaults={
            'rpm_limit':           200,
            'tpm_limit':           200000,
            'stats_window_minutes': 5,
            'backoff_max_retries': 5,
            'backoff_base_delay':  1.0,
            'backoff_max_delay':   60.0,
            'backoff_jitter':      True,
            'active_strategies':   '',
        })
        r = redis_sync.from_url(REDIS_URL)
        obj._sync_to_redis(r)
        SimUser.sync_all_to_redis(r)
        r.close()
        return obj


class SimUser(models.Model):
    MODE_NORMAL  = 'normal'
    MODE_NOISY   = 'noisy'
    MODE_SPAMMER = 'spammer'
    MODE_CHOICES = [
        (MODE_NORMAL,  'Normal'),
        (MODE_NOISY,   'Noisy'),
        (MODE_SPAMMER, 'Spammer'),
    ]

    id         = models.PositiveIntegerField(primary_key=True)
    mode       = models.CharField(max_length=10, choices=MODE_CHOICES, default=MODE_NORMAL)
    vkey_value = models.CharField(max_length=128, blank=True, default='')
    vkey_id    = models.CharField(max_length=128, blank=True, default='')

    class Meta:
        verbose_name_plural = 'Simulated Users'
        ordering = ['id']

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        pipe = r.pipeline()
        pipe.srem('config:noisy_users',    self.id)
        pipe.srem('config:spammer_users',  self.id)
        if self.mode == self.MODE_NOISY:
            pipe.sadd('config:noisy_users',   self.id)
        elif self.mode == self.MODE_SPAMMER:
            pipe.sadd('config:spammer_users', self.id)
        if self.vkey_value:
            pipe.set(f'config:vkey:{self.id}',    self.vkey_value)
            pipe.set(f'config:vkey_id:{self.id}', self.vkey_id)
        else:
            pipe.delete(f'config:vkey:{self.id}', f'config:vkey_id:{self.id}')
        pipe.execute()
        r.close()

    @classmethod
    def sync_all_to_redis(cls, r):
        """Rebuild all user-related Redis keys from the DB in a single pipeline."""
        users = list(cls.objects.all())
        pipe = r.pipeline()
        # Rebuild mode sets from scratch
        pipe.delete('config:noisy_users', 'config:spammer_users')
        noisy_ids   = [u.id for u in users if u.mode == cls.MODE_NOISY]
        spammer_ids = [u.id for u in users if u.mode == cls.MODE_SPAMMER]
        if noisy_ids:
            pipe.sadd('config:noisy_users',   *noisy_ids)
        if spammer_ids:
            pipe.sadd('config:spammer_users', *spammer_ids)
        # Sync virtual keys
        for u in users:
            if u.vkey_value:
                pipe.set(f'config:vkey:{u.id}',    u.vkey_value)
                pipe.set(f'config:vkey_id:{u.id}', u.vkey_id)
            else:
                pipe.delete(f'config:vkey:{u.id}', f'config:vkey_id:{u.id}')
        pipe.execute()


class VirtualKeySettings(models.Model):
    rpm_per_user  = models.PositiveIntegerField(default=50)
    tpm_per_user  = models.PositiveIntegerField(default=50000)
    budget_limit  = models.FloatField(default=1.0)
    budget_reset  = models.CharField(max_length=10, default='24h')

    class Meta:
        verbose_name = verbose_name_plural = 'Virtual Key Settings'

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(pk=1, defaults={
            'rpm_per_user': 50,
            'tpm_per_user': 50000,
            'budget_limit': 1.0,
            'budget_reset': '24h',
        })
        return obj
