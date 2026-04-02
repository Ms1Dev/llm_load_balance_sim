from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='Config',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time_scale', models.PositiveIntegerField(default=4, help_text='Compresses the rate-limit window. 4 = 60s window behaves like 15s.')),
            ],
            options={
                'verbose_name': 'Configuration',
                'verbose_name_plural': 'Configuration',
            },
        ),
    ]
