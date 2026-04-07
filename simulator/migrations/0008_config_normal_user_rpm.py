from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('simulator', '0007_virtualkeysettings_pro_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='config',
            name='normal_user_rpm',
            field=models.PositiveIntegerField(default=6, help_text='Average requests per minute for normal (non-spammer, non-bursty) users.'),
        ),
    ]
