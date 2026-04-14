from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("simulator", "0008_config_normal_user_rpm"),
    ]

    operations = [
        migrations.AddField(
            model_name="simuser",
            name="spend",
            field=models.FloatField(default=0.0),
        ),
    ]
