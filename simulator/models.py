import os

import redis as redis_sync
from django.db import models

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")


class Config(models.Model):
    rpm_limit = models.PositiveIntegerField(
        default=500,
        help_text="Maximum requests per minute allowed by the mock LLM.",
    )
    tpm_limit = models.PositiveIntegerField(
        default=200000,
        help_text="Maximum tokens per minute allowed by the mock LLM.",
    )
    stats_window_minutes = models.PositiveIntegerField(
        default=1,
        help_text="Legacy field; user tiles use a fixed 60s window for RPM and avg latency.",
    )
    normal_user_rpm = models.PositiveIntegerField(
        default=6,
        help_text="Average requests per minute for normal (non-spammer, non-bursty) users.",
    )
    # Comma-separated active strategy names, e.g. 'backoff,throttle'
    active_strategies = models.CharField(max_length=64, blank=True, default="backoff")
    usage_pattern = models.CharField(max_length=32, default="sine_wave")

    class Meta:
        verbose_name = verbose_name_plural = "Configuration"

    def _sync_to_redis(self, r):
        pipe = r.pipeline()
        pipe.mset(
            {
                "config:rpm_limit": self.rpm_limit,
                "config:tpm_limit": self.tpm_limit,
                "config:normal_user_rpm": self.normal_user_rpm,
                "config:usage_pattern": self.usage_pattern,
            }
        )
        strategies = [s for s in self.active_strategies.split(",") if s]
        pipe.delete("config:strategies")
        if strategies:
            pipe.sadd("config:strategies", *strategies)
        pipe.execute()

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        self._sync_to_redis(r)
        r.close()

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(
            pk=1,
            defaults={
                "rpm_limit": 500,
                "tpm_limit": 200000,
                "stats_window_minutes": 5,
                "normal_user_rpm": 6,
                "active_strategies": "backoff",
                "usage_pattern": "sine_wave",
            },
        )
        r = redis_sync.from_url(REDIS_URL)
        obj._sync_to_redis(r)
        SimUser.sync_all_to_redis(r)
        r.close()
        return obj


class SimUser(models.Model):
    MODE_NORMAL = "normal"
    MODE_BURSTY = "bursty"
    MODE_SPAMMER = "spammer"
    MODE_CHOICES = [
        (MODE_NORMAL, "Normal"),
        (MODE_BURSTY, "Bursty"),
        (MODE_SPAMMER, "Spammer"),
    ]

    TIER_BASIC = "basic"
    TIER_PRO = "pro"
    TIER_CHOICES = [
        (TIER_BASIC, "Basic"),
        (TIER_PRO, "Pro"),
    ]

    id = models.PositiveIntegerField(primary_key=True)
    mode = models.CharField(max_length=10, choices=MODE_CHOICES, default=MODE_NORMAL)
    tier = models.CharField(max_length=8, choices=TIER_CHOICES, default=TIER_BASIC)
    vkey_value = models.CharField(max_length=128, blank=True, default="")
    vkey_id = models.CharField(max_length=128, blank=True, default="")
    spend = models.FloatField(default=0.0)

    class Meta:
        verbose_name_plural = "Simulated Users"
        ordering = ["id"]

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        r = redis_sync.from_url(REDIS_URL)
        pipe = r.pipeline()
        pipe.srem("config:bursty_users", self.id)
        pipe.srem("config:spammer_users", self.id)
        pipe.srem("config:pro_users", self.id)
        if self.mode == self.MODE_BURSTY:
            pipe.sadd("config:bursty_users", self.id)
        elif self.mode == self.MODE_SPAMMER:
            pipe.sadd("config:spammer_users", self.id)
        if self.tier == self.TIER_PRO:
            pipe.sadd("config:pro_users", self.id)
        if self.vkey_value:
            pipe.set(f"config:vkey:{self.id}", self.vkey_value)
            pipe.set(f"config:vkey_id:{self.id}", self.vkey_id)
        else:
            pipe.delete(f"config:vkey:{self.id}", f"config:vkey_id:{self.id}")
        pipe.execute()
        r.close()

    @classmethod
    def sync_all_to_redis(cls, r):
        """Rebuild all user-related Redis keys from the DB in a single pipeline."""
        users = list(cls.objects.all())
        pipe = r.pipeline()
        # Rebuild mode and tier sets from scratch
        pipe.delete("config:bursty_users", "config:spammer_users", "config:pro_users")
        bursty_ids = [u.id for u in users if u.mode == cls.MODE_BURSTY]
        spammer_ids = [u.id for u in users if u.mode == cls.MODE_SPAMMER]
        pro_ids = [u.id for u in users if u.tier == cls.TIER_PRO]
        if bursty_ids:
            pipe.sadd("config:bursty_users", *bursty_ids)
        if spammer_ids:
            pipe.sadd("config:spammer_users", *spammer_ids)
        if pro_ids:
            pipe.sadd("config:pro_users", *pro_ids)
        # Sync virtual keys
        for u in users:
            if u.vkey_value:
                pipe.set(f"config:vkey:{u.id}", u.vkey_value)
                pipe.set(f"config:vkey_id:{u.id}", u.vkey_id)
            else:
                pipe.delete(f"config:vkey:{u.id}", f"config:vkey_id:{u.id}")
        pipe.execute()


class VirtualKeySettings(models.Model):
    # Basic tier settings
    requests_per_user = models.PositiveIntegerField(default=10)
    requests_reset = models.CharField(max_length=10, default="1m")
    tokens_per_user = models.PositiveIntegerField(default=10000)
    tokens_reset = models.CharField(max_length=10, default="1m")
    budget_limit = models.FloatField(default=1.0)
    budget_reset = models.CharField(max_length=10, default="24h")
    # Pro tier settings
    pro_requests_per_user = models.PositiveIntegerField(default=20)
    pro_requests_reset = models.CharField(max_length=10, default="1m")
    pro_tokens_per_user = models.PositiveIntegerField(default=20000)
    pro_tokens_reset = models.CharField(max_length=10, default="1m")
    pro_budget_limit = models.FloatField(default=5.0)
    pro_budget_reset = models.CharField(max_length=10, default="24h")

    class Meta:
        verbose_name = verbose_name_plural = "Virtual Key Settings"

    def save(self, *args, **kwargs):
        self.pk = 1  # singleton
        super().save(*args, **kwargs)

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(
            pk=1,
            defaults={
                "requests_per_user": 10,
                "requests_reset": "1m",
                "tokens_per_user": 10000,
                "tokens_reset": "1m",
                "budget_limit": 1.0,
                "budget_reset": "24h",
                "pro_requests_per_user": 20,
                "pro_requests_reset": "1m",
                "pro_tokens_per_user": 20000,
                "pro_tokens_reset": "1m",
                "pro_budget_limit": 5.0,
                "pro_budget_reset": "24h",
            },
        )
        return obj
