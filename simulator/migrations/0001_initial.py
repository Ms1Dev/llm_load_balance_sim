from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Config",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "time_scale",
                    models.PositiveIntegerField(
                        default=4,
                        help_text="Compresses the rate-limit window. 4 = 60s window behaves like 15s.",
                    ),
                ),
                (
                    "rpm_limit",
                    models.PositiveIntegerField(
                        default=30,
                        help_text="Maximum requests per minute allowed by the vLLM.",
                    ),
                ),
                (
                    "tpm_limit",
                    models.PositiveIntegerField(
                        default=10000,
                        help_text="Maximum tokens per minute allowed by the vLLM.",
                    ),
                ),
            ],
            options={
                "verbose_name": "Configuration",
                "verbose_name_plural": "Configuration",
            },
        ),
    ]
