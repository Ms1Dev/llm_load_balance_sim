from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("simulator", "0006_simuser_tier"),
    ]

    operations = [
        migrations.AddField(
            model_name="virtualkeysettings",
            name="pro_rpm_per_user",
            field=models.PositiveIntegerField(default=100),
        ),
        migrations.AddField(
            model_name="virtualkeysettings",
            name="pro_tpm_per_user",
            field=models.PositiveIntegerField(default=100000),
        ),
        migrations.AddField(
            model_name="virtualkeysettings",
            name="pro_budget_limit",
            field=models.FloatField(default=5.0),
        ),
        migrations.AddField(
            model_name="virtualkeysettings",
            name="pro_budget_reset",
            field=models.CharField(default="24h", max_length=10),
        ),
    ]
