from django.db import migrations


def create_sim_users(apps, schema_editor):
    SimUser = apps.get_model("simulator", "SimUser")
    SimUser.objects.bulk_create(
        [
            SimUser(id=uid, mode="normal", vkey_value="", vkey_id="")
            for uid in range(1, 101)
        ]
    )


def reverse_sim_users(apps, schema_editor):
    SimUser = apps.get_model("simulator", "SimUser")
    SimUser.objects.all().delete()


class Migration(migrations.Migration):
    dependencies = [
        ("simulator", "0004_config_backoff_simuser_vkeysettings"),
    ]

    operations = [
        migrations.RunPython(create_sim_users, reverse_sim_users),
    ]
