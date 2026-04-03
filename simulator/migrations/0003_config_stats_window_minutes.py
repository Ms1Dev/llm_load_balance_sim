from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('simulator', '0002_remove_config_time_scale'),
    ]

    operations = [
        migrations.AddField(
            model_name='config',
            name='stats_window_minutes',
            field=models.PositiveIntegerField(default=5, help_text='Rolling window in minutes for per-user stats on the dashboard.'),
        ),
    ]
