from celery import Celery
from celery.schedules import crontab
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('resto_sync')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

app.conf.beat_schedule = {
    'auto-sync-every-5-min': {
        'task': 'sync.tasks.auto_sync_all_hospitals',
        'schedule': 300.0,
    },
}