
# sync/models.py
from django.db import models
from django.contrib.contenttypes.models import ContentType

class SyncLog(models.Model):
    hospital_id = models.IntegerField(null=True, blank=True)
    direction = models.CharField(max_length=10)  # UPLOAD / DOWNLOAD
    status = models.CharField(max_length=20, default="PENDING")  # PENDING / SUCCESS / FAILED / CONFLICT
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, null=True)
    object_id = models.IntegerField(null=True, blank=True)
    operation = models.CharField(max_length=10)  # CREATE / UPDATE
    data_snapshot = models.JSONField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    remote_updatedAt = models.DateTimeField(null=True, blank=True)
    local_updatedAt = models.DateTimeField(null=True, blank=True)
    synced_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)


class SyncQueueItem(models.Model):
    uuid = models.UUIDField()
    model_name = models.CharField(max_length=200)
    hospital_id = models.IntegerField(null=True, blank=True)
    operation = models.CharField(max_length=10)  # CREATE / UPDATE
    data = models.JSONField()
    sync_version = models.IntegerField(default=1)
    status = models.CharField(max_length=20, default="PENDING")  # PENDING / PROCESSING / SUCCESS / FAILED
    created_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)
2️⃣ Service de synchronisation
# sync/service.py
import logging
from django.utils import timezone
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.forms.models import model_to_dict

class SyncService:
    def __init__(self, hospital_id, config=None, session=None):
        self.hospital_id = hospital_id
        self.config = config
        self.session = session

    # -------------------
    # Sérialisation
    # -------------------
    def _serialize_object(self, obj):
        data = model_to_dict(obj)
        for k, v in data.items():
            if hasattr(v, "isoformat"):
                data[k] = v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        data["uuid"] = getattr(obj, "uuid", None)
        data["sync_version"] = getattr(obj, "sync_version", 1)
        return data

    # -------------------
    # Nettoyage données
    # -------------------
    def _clean_data_for_create(self, data):
        excluded_fields = ["id", "createdAt", "updatedAt", "timeAt"]
        return {k: v for k, v in data.items() if k not in excluded_fields}

    def _clean_data_for_update(self, data):
        excluded_fields = ["id", "code", "createdAt", "updatedAt", "timeAt", "hospital_id"]
        return {k: v for k, v in data.items() if k not in excluded_fields}

    # -------------------
    # Upload queue
    # -------------------
    def _upload_object(self, obj, model_name):
        from .models import SyncQueueItem
        uuid_val = getattr(obj, "uuid", None)
        if not uuid_val:
            return "failed"
        SyncQueueItem.objects.create(
            uuid=uuid_val,
            model_name=model_name,
            hospital_id=getattr(obj, "hospital_id", None),
            operation="UPDATE" if obj.pk else "CREATE",
            data=self._serialize_object(obj),
            sync_version=getattr(obj, "sync_version", 1)
        )
        from .tasks import process_sync_queue
        process_sync_queue.apply_async()
        return "queued"

    def _upload_batch(self, objects, model_name):
        results = {"queued": 0, "failed": 0}
        for obj in objects:
            try:
                r = self._upload_object(obj, model_name)
                if r == "queued":
                    results["queued"] += 1
            except Exception:
                results["failed"] += 1
        return results

    # -------------------
    # Download queue
    # -------------------
    def _queue_download_object(self, remote_data, Model):
        from .models import SyncQueueItem
        uuid_val = remote_data.get("uuid")
        if not uuid_val:
            return "failed"
        sync_version = remote_data.get("sync_version", 1)
        has_hospital = hasattr(Model, "hospital_id")
        SyncQueueItem.objects.create(
            uuid=uuid_val,
            model_name=Model._meta.label,
            hospital_id=self.hospital_id if has_hospital else None,
            operation="UPDATE" if Model.objects.filter(uuid=uuid_val).exists() else "CREATE",
            data=remote_data,
            sync_version=sync_version
        )
        from .tasks import process_sync_queue
        process_sync_queue.apply_async()
        return "queued"

    def _queue_download_batch(self, remote_objects, Model):
        results = {"queued": 0, "failed": 0}
        for remote_data in remote_objects:
            try:
                r = self._queue_download_object(remote_data, Model)
                if r == "queued":
                    results["queued"] += 1
            except Exception:
                results["failed"] += 1
        return results
3️⃣ Tâches Celery
# sync/tasks.py
from celery import shared_task
from django.apps import apps
from django.utils import timezone
from .models import SyncQueueItem
from .service import SyncService

@shared_task(bind=True)
def process_sync_queue(self, batch_size=50):
    items = SyncQueueItem.objects.filter(status="PENDING").order_by("created_at")[:batch_size]
    total_results = {"success": 0, "failed": 0}

    for item in items:
        try:
            app_label, model_cls = item.model_name.split(".")
            Model = apps.get_model(app_label, model_cls)
            service = SyncService(hospital_id=item.hospital_id)

            clean_data = service._clean_data_for_create(item.data) if item.operation=="CREATE" else service._clean_data_for_update(item.data)
            has_hospital = hasattr(Model, "hospital_id")
            if has_hospital and "hospital_id" not in clean_data:
                clean_data["hospital_id"] = item.hospital_id

            local_obj = Model.objects.filter(uuid=item.uuid).first()
            if item.operation=="CREATE" and not local_obj:
                Model.objects.create(**clean_data)
            elif local_obj:
                for k,v in clean_data.items():
                    setattr(local_obj, k, v)
                local_obj.save()

            item.status = "SUCCESS"
            item.processed_at = timezone.now()
            item.save()
            total_results["success"] += 1
        except Exception:
            item.status = "FAILED"
            item.save()
            total_results["failed"] += 1

    # Relancer la tâche si la queue n’est pas vide
    if SyncQueueItem.objects.filter(status="PENDING").exists():
        process_sync_queue.apply_async(countdown=5)

    return total_results
4️⃣ Vues API batch
# sync/views.py
from rest_framework.decorators import action
from rest_framework.response import Response
from django.apps import apps
from .service import SyncService
from .tasks import process_sync_queue

class SyncViewSet(...):
    @action(detail=False, methods=["post"], url_path=r'model/(?P<models>[^/.]+)', url_name='sync-model-batch-create')
    def sync_model_batch_create(self, request, models=None):
        hospital_id = request.data.get("hospital_id")
        objects_data = request.data.get("objects", [])
        if not hospital_id or not objects_data:
            return Response({"error": "hospital_id ou objects manquant"}, status=400)

        app_label, model_cls = "app_label", models  # adapter selon ton projet
        Model = apps.get_model(app_label, model_cls)
        service = SyncService(hospital_id=hospital_id)
        results = service._queue_download_batch(objects_data, Model)

        # Lancer worker Celery
        process_sync_queue.apply_async()

        return Response({"message": "Batch queued for processing", "results": results}, status=201)