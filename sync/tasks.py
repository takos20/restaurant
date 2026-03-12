# sync/tasks.py
from celery import shared_task
from django.utils import timezone
from .models import SyncConfig
from .sync_service import SyncService
import logging

logger = logging.getLogger(__name__)

from celery import shared_task
from django.apps import apps
from django.utils import timezone

from .models import SyncQueueItem


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    max_retries=5
)
def process_sync_batch(self, queue_id):

    item = SyncQueueItem.objects.get(id=queue_id)

    try:

        item.status = "PROCESSING"
        item.save(update_fields=["status"])

        app_label, model_name = item.model_name.split(".")

        Model = apps.get_model(app_label, model_name)

        objects = item.objects_data

        for data in objects:

            uuid = data.get("uuid")

            if not uuid:
                raise Exception("UUID manquant")

            obj = Model.objects.filter(uuid=uuid).first()

            if obj:

                for k, v in data.items():
                    setattr(obj, k, v)

                obj.full_clean()
                obj.save()

            else:

                obj = Model(**data)

                obj.full_clean()
                obj.save()

        item.status = "SUCCESS"

        item.processed_at = timezone.now()

        item.save(update_fields=["status", "processed_at"])

        return "success"

    except Exception as exc:

        item.retry_count += 1

        item.error_message = str(exc)

        if item.retry_count >= item.max_retries:

            item.status = "FAILED"

        item.save(
            update_fields=[
                "retry_count",
                "status",
                "error_message"
            ]
        )

        raise self.retry(exc=exc)
@shared_task
def auto_sync_all_hospitals():
    """Synchroniser automatiquement tous les hôpitaux"""
    configs = SyncConfig.objects.filter(is_active=True, auto_sync=True)
    
    results = []
    for config in configs:
        try:
            result = auto_sync_hospital.delay(config.hospital_id)
            results.append({'hospital_id': config.hospital_id, 'task_id': result.id})
        except Exception as e:
            logger.error(f"Erreur auto-sync hospital {config.hospital_id}: {e}")
    
    return results


@shared_task
def auto_sync_hospital(hospital_id):
    """Synchroniser un hôpital spécifique"""
    try:
        config = SyncConfig.objects.get(hospital_id=hospital_id)
        
        if config.last_sync_upload:
            elapsed = (timezone.now() - config.last_sync_upload).total_seconds()
            if elapsed < config.sync_interval:
                return {'skipped': True}
        
        sync_service = SyncService(hospital_id)
        upload_results = sync_service.upload_changes(force=False)
        download_results = sync_service.download_changes(force=False)
        
        return {
            'hospital_id': hospital_id,
            'upload': upload_results,
            'download': download_results,
            'timestamp': timezone.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Erreur auto_sync_hospital {hospital_id}: {e}")
        raise