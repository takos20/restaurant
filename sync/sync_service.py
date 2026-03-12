# sync/services/sync_service.py
import requests
import logging
from django.db import transaction
from django.apps import apps
from django.utils import timezone
from datetime import datetime, timedelta

from hospital.models import SyncQueueItem
from .models import SyncLog, SyncConfig
from django.utils.dateparse import parse_datetime
from django.contrib.contenttypes.models import ContentType
import time
import requests
MAX_RETRIES = 5
RETRY_DELAY = 2

def retry_operation(func):

    def wrapper(*args, **kwargs):

        retries = 0

        while retries < MAX_RETRIES:

            try:
                return func(*args, **kwargs)

            except Exception as e:

                retries += 1

                if retries >= MAX_RETRIES:
                    raise e

                delay = RETRY_DELAY ** retries
                time.sleep(delay)

    return wrapper
class SyncService:
    def __init__(self, hospital_id, config):
        self.hospital_id = hospital_id
        self.config = config
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        session.headers.update({
            "X-API-KEY": self.config.api_token,
            "Content-Type": "application/json"
        })
        return session

    # ================= UPLOAD (Local → Remote) =================
    def upload_changes(self, force=False):
        """
        Envoyer les modifications locales vers le serveur distant
        
        Args:
            force: Forcer la sync même si auto_sync est désactivé
        """
        if not self.config.is_active:
            logging.getLogger('errors_file').info(msg={"error": f"Sync désactivée pour hospital {self.hospital_id}"})
            return
        
        if not self.config.auto_sync and not force:
            logging.getLogger('info_file').info(msg="Auto-sync désactivé")
            return
        
        results = {
            'success': 0,
            'failed': 0,
            'conflicts': 0,
            'skipped': 0
        }
        
        try:
            # Pour chaque modèle à synchroniser
            for model_name in self.config.models_to_sync:
                try:
                    model_results = self._upload_model(model_name)
                    results['success'] += model_results['success']
                    results['failed'] += model_results['failed']
                    results['conflicts'] += model_results['conflicts']
                    results['skipped'] += model_results['skipped']
                except Exception as e:
                    logging.getLogger('errors_file').info(msg={"error": f"Erreur upload {model_name}: {e}"})
                    results['failed'] += 1
            
            # Mettre à jour le timestamp
            self.config.last_sync_upload = timezone.now()
            self.config.last_sync_version = response["max_version"]
            self.config.save()
            logging.getLogger('errors_file').info(msg={"error": f"Upload terminé: {results}"})
            return results
            
        except Exception as e:
            logging.getLogger('errors_file').info(msg={"error": f"Erreur upload_changes: {e}"})
            raise
    
    def _upload_model(self, model_name):
        """Uploader les changements d'un modèle spécifique"""
        results = {'success': 0, 'failed': 0, 'conflicts': 0, 'skipped': 0}

        try:
            app_label, model = model_name.split(".")
            Model = apps.get_model(app_label, model)
        except LookupError:
            logging.getLogger('errors_file').info(
                msg={"error": f"Modèle {model_name} introuvable"}
            )
            return results

        # Vérifier les champs du modèle
        model_fields = [f.name for f in Model._meta.get_fields()]
        has_hospital = 'hospital' in model_fields or 'hospital_id' in model_fields
        has_deleted = 'deleted' in model_fields
        has_updated = 'updatedAt' in model_fields

        # Dernière synchronisation
        last_sync = self.config.last_sync_upload or timezone.now() - timedelta(days=365)

        queryset = Model.objects.all()

        # Filtre hospital seulement si le champ existe
        if has_hospital:
            queryset = queryset.filter(hospital_id=self.hospital_id)

        # Filtre updatedAt seulement si présent
        if has_updated:
            queryset = queryset.filter(updatedAt__gt=last_sync)

        # Filtre deleted seulement si présent
        if has_deleted:
            queryset = queryset.filter(deleted=False)

        if queryset.count() > 20:
            batch_results = self._upload_batch(queryset, model)

            for key in results:
                results[key] += batch_results.get(key, 0)

            return results
        else:
            for obj in queryset:
                try:
                    result = self._upload_object(obj, model)
                    results[result] += 1
                except Exception as e:
                    logging.getLogger('errors_file').info(
                        msg={"error": f"Erreur upload {model} #{obj.id}: {e}"}
                    )
                    results['failed'] += 1

            return results
        

    @retry_operation
    def _upload_batch(self, queryset, model_name):
        batch_size = 100
        results = {'success': 0, 'failed': 0}

        for i in range(0, queryset.count(), batch_size):
            batch = queryset[i:i+batch_size]

            payload = []

            for obj in batch:
                data = self._serialize_object(obj)
                data['uuid'] = str(obj.uuid)
                data['sync_version'] = obj.sync_version
                if hasattr(obj, 'hospital_id'):
                    data['hospital_id'] = obj.hospital_id
                    data.pop("hospital", None)
                payload.append(data)

            url = f"{self.config.remote_api_url}/sync/model/{model_name.lower()}/batch"

            response = self.session.post(url, json={"objects": payload})

            if response.status_code == 200:
                results['success'] += len(batch)
            else:
                results['failed'] += len(batch)

        return results
    
    @retry_operation
    def _upload_object(self, obj, model_name):
        """Uploader un objet individuel"""

        Model = obj.__class__
        model_fields = [f.name for f in Model._meta.get_fields()]
        has_hospital = 'hospital' in model_fields or 'hospital_id' in model_fields

        # Vérifier s'il existe déjà un log en attente
        existing_log = SyncLog.objects.filter(
            hospital_id=self.hospital_id,
            content_type__model=model_name.lower(),
            object_id=obj.id,
            status='PENDING',
            direction='UPLOAD'
        ).first()

        if existing_log:
            return 'skipped'

        # Créer un log
        sync_log = SyncLog.objects.create(
            hospital_id=self.hospital_id,
            direction='UPLOAD',
            status='PENDING',
            content_object=obj,
            operation='UPDATE' if obj.pk else 'CREATE',
            data_snapshot=self._serialize_object(obj),
            local_updatedAt=obj.updatedAt if hasattr(obj, 'updatedAt') else timezone.now()
        )

        try:
            # Préparer les données
            data = self._serialize_object(obj)
            data['uuid'] = str(obj.uuid)              # obligatoire
            data['sync_version'] = obj.sync_version  # pour delta sync

            # Ajouter hospital_id seulement si le modèle l'utilise
            if has_hospital:
                data['hospital_id'] = self.hospital_id

                # supprimer hospital s'il existe
                if "hospital" in data:
                    data.pop("hospital")

            # URL API
            url = f"{self.config.remote_api_url}/sync/model/{model_name.lower()}"

            response = self.session.post(url, json=data)

            if response.status_code in [200, 201]:
                sync_log.status = 'SUCCESS'
                sync_log.synced_at = timezone.now()
                sync_log.remote_updated_at = timezone.now()
                sync_log.save()
                return 'success'

            elif response.status_code == 409:  # conflit
                sync_log.status = 'CONFLICT'
                sync_log.error_message = response.json().get('message', 'Conflit détecté')
                sync_log.save()

                if self.config.conflict_strategy != 'MANUAL':
                    self._resolve_conflict(sync_log, response.json())
                    return 'success'

                return 'conflict'

            else:
                sync_log.status = 'FAILED'
                sync_log.error_message = f"HTTP {response.status_code}: {response.text}"
                sync_log.save()
                return 'failed'

        except Exception as e:
            sync_log.status = 'FAILED'
            sync_log.error_message = str(e)
            sync_log.save()
            return 'failed'
    # ================= DOWNLOAD (Remote → Local) =================
    def download_changes(self, force=False):
        """
        Télécharger les modifications du serveur distant
        
        Args:
            force: Forcer la sync même si auto_sync est désactivé
        """
        if not self.config.is_active:
            logging.getLogger('errors_file').info(msg={"error": f"Sync désactivée pour hospital {self.hospital_id}"})
            return
        
        if not self.config.auto_sync and not force:
            logging.getLogger('info_file').info(msg="Auto-sync désactivé")
            return
        
        results = {
            'success': 0,
            'failed': 0,
            'conflicts': 0,
            'skipped': 0
        }
        
        try:
            # Pour chaque modèle à synchroniser
            for model_name in self.config.models_to_sync:
                try:
                    model_results = self._download_model(model_name)
                    results['success'] += model_results['success']
                    results['failed'] += model_results['failed']
                    results['conflicts'] += model_results['conflicts']
                    results['skipped'] += model_results['skipped']
                except Exception as e:
                    logging.getLogger('errors_file').info(msg={"error": f"Erreur download {model_name}: {e}"})
                    results['failed'] += 1
            
            # Mettre à jour le timestamp
            self.config.last_sync_download = timezone.now()
            self.config.save()
            logging.getLogger('errors_file').info(msg={"error": f"Download terminé: {results}"})
            return results
            
        except Exception as e:
            logging.getLogger('errors_file').info(msg={"error": f"Erreur download_changes: {e}"})
            raise

    @retry_operation
    def _download_batch(self, remote_objects, Model):
        """
        Télécharger et appliquer un batch d'objets distants
        """
        results = {"success": 0, "skipped": 0, "failed": 0, "conflicts": 0}
        model_fields = [f.name for f in Model._meta.get_fields()]
        has_hospital = 'hospital' in model_fields or 'hospital_id' in model_fields

        # Récupérer tous les UUID existants en une requête
        uuids = [obj.get("uuid") for obj in remote_objects if obj.get("uuid")]
        if has_hospital:
            local_objs = {o.uuid: o for o in Model.objects.filter(uuid__in=uuids, hospital_id=self.hospital_id)}
        else:
            local_objs = {o.uuid: o for o in Model.objects.filter(uuid__in=uuids)}
        print(remote_objects)
        for remote_data in remote_objects:
            uuid = remote_data.get("uuid")
            if not uuid:
                results["failed"] += 1
                continue

            local_obj = local_objs.get(uuid)
            operation = "UPDATE" if local_obj else "CREATE"

            # Création du log
            try:
                sync_log = SyncLog.objects.create(
                    hospital_id=self.hospital_id,
                    direction='DOWNLOAD',
                    status='PENDING',
                    content_type=ContentType.objects.get_for_model(Model),
                    object_id=local_obj.id if local_obj else None,
                    operation=operation,
                    data_snapshot=remote_data,
                    remote_updatedAt=parse_datetime(remote_data.get("updatedAt")),
                    local_updatedAt=local_obj.updatedAt if local_obj else timezone.now()
                )
            except Exception as e:
                logging.getLogger('errors_file').info(msg={"error": f"Erreur create synclog: {e}"})
                results["failed"] += 1
                continue

            try:
                # Vérifier conflits
                if local_obj and operation == "UPDATE":
                    remote_updated = parse_datetime(remote_data.get("updatedAt"))
                    local_updated = local_obj.updatedAt

                    if remote_updated and local_updated and local_updated > remote_updated:
                        sync_log.status = 'CONFLICT'
                        sync_log.error_message = 'Version locale plus récente'
                        sync_log.save()

                        results["conflicts"] += 1

                        if self.config.conflict_strategy == 'LOCAL_WINS':
                            results["skipped"] += 1
                            continue
                        elif self.config.conflict_strategy == 'REMOTE_WINS':
                            pass
                        else:
                            results["conflicts"] += 1
                            continue

                # Appliquer les données
                if operation == "CREATE":
                    clean_data = self._clean_data_for_create(remote_data)
                    if has_hospital:
                        clean_data['hospital_id'] = self.hospital_id
                    local_obj = Model.objects.create(**clean_data)
                else:
                    clean_data = self._clean_data_for_update(remote_data)
                    for key, value in clean_data.items():
                        setattr(local_obj, key, value)
                    local_obj.save()

                sync_log.status = 'SUCCESS'
                sync_log.synced_at = timezone.now()
                sync_log.object_id = local_obj.id
                sync_log.save()

                results["success"] += 1

            except Exception as e:
                sync_log.status = 'FAILED'
                sync_log.error_message = str(e)
                sync_log.save()
                results["failed"] += 1

        return results
    def _download_model(self, model_name):
        """Télécharger les changements d'un modèle spécifique"""
        results = {'success': 0, 'failed': 0, 'conflicts': 0, 'skipped': 0}
        
        try:
            app_label, model = model_name.split(".")
            Model = apps.get_model(app_label, model)
        except LookupError:
            logging.getLogger('errors_file').info(
                msg={"error": f"Modèle {model} introuvable"}
            )
            return results
        
        # Vérifier si le modèle possède hospital ou hospital_id
        model_fields = [f.name for f in Model._meta.get_fields()]
        has_hospital = 'hospital' in model_fields or 'hospital_id' in model_fields

        # Récupérer les modifications depuis le serveur
        last_version = self.config.last_sync_version or 0
        params = {"since_version": last_version}

        url = f"{self.config.remote_api_url}/sync/model/{model.lower()}"

        # Ajouter hospital_id seulement si le modèle le supporte
        if has_hospital:
            params['hospital_id'] = self.hospital_id

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            remote_objects = response.json()

            batch_results = self._download_batch(
                remote_objects.get("results", []),
                Model
            )
            for key in results:
                results[key] += batch_results.get(key, 0)
            # Mettre à jour la dernière version sync

            if remote_objects:
                max_version = max(obj['sync_version'] for obj in remote_objects)
                self.config.last_sync_version = max_version
                self.config.save()
        except Exception:

            results["failed"] += 1

        return results
    @retry_operation
    @transaction.atomic
    def _download_object(self, remote_data, Model):
        """Télécharger et appliquer un objet distant"""

        model_fields = [f.name for f in Model._meta.get_fields()]
        has_hospital = 'hospital' in model_fields or 'hospital_id' in model_fields

        # Identifier l'objet local
        # code = remote_data.get('code')
        # if not code:
        #     logging.getLogger('errors_file').info(
        #         msg={"error": f"Objet sans code: {remote_data}"}
        #     )
        #     return 'failed'
        uuid = remote_data.get("uuid")
        try:
            if has_hospital:
                local_obj = Model.objects.filter(uuid=uuid, hospital_id=self.hospital_id).first()
            else:
                local_obj = Model.objects.filter(uuid=uuid).first()

            operation = 'UPDATE'

        except Model.DoesNotExist:
            local_obj = None
            operation = 'CREATE'

        # Création du log
        try:
            sync_log = SyncLog.objects.create(
                hospital_id=self.hospital_id,
                direction='DOWNLOAD',
                status='PENDING',
                content_type=ContentType.objects.get_for_model(Model),
                object_id=local_obj.id if local_obj else None,
                operation=operation,
                data_snapshot=remote_data,
                remote_updatedAt=parse_datetime(remote_data.get("updatedAt")),
                local_updatedAt=local_obj.updatedAt if local_obj else timezone.now()
            )
        except Exception as e:
            logging.getLogger('errors_file').info(
                msg={"error": f"Erreur create synclog: {e}"}
            )

        try:
            # Vérifier conflits
            if local_obj and operation == 'UPDATE':
                remote_updated = parse_datetime(remote_data.get("updatedAt"))
                local_updated = local_obj.updatedAt

                if remote_updated and local_updated and local_updated > remote_updated:
                    sync_log.status = 'CONFLICT'
                    sync_log.error_message = 'Version locale plus récente'
                    sync_log.save()

                    if self.config.conflict_strategy == 'LOCAL_WINS':
                        return 'skipped'

                    elif self.config.conflict_strategy == 'REMOTE_WINS':
                        pass

                    else:
                        return 'conflict'

            # Appliquer les données
            if operation == 'CREATE':
                clean_data = self._clean_data_for_create(remote_data)

                if has_hospital:
                    clean_data['hospital_id'] = self.hospital_id

                local_obj = Model.objects.create(**clean_data)

            else:
                clean_data = self._clean_data_for_update(remote_data)

                for key, value in clean_data.items():
                    setattr(local_obj, key, value)

                local_obj.save()

            sync_log.status = 'SUCCESS'
            sync_log.synced_at = timezone.now()
            sync_log.object_id = local_obj.id
            sync_log.save()

            return 'success'

        except Exception as e:
            sync_log.status = 'FAILED'
            sync_log.error_message = str(e)
            sync_log.save()

            return 'failed'
    # ==================== UTILITAIRES ====================
    
    def _serialize_object(self, obj):
        """Sérialiser un objet Django en JSON, avec UUID et Delta Sync"""
        from django.forms.models import model_to_dict
        from datetime import datetime
        from django.utils import timezone

        # Sérialisation de base
        data = model_to_dict(obj)

        # Ajouter UUID et sync_version si existants
        if hasattr(obj, "uuid"):
            data["uuid"] = str(obj.uuid)
        if hasattr(obj, "sync_version"):
            data["sync_version"] = obj.sync_version

        # Ajouter hospital_id si présent
        if hasattr(obj, "hospital_id"):
            data["hospital_id"] = obj.hospital_id
            data.pop("hospital", None)  # supprimer objet lié pour éviter erreur JSON

        # Convertir les dates en ISO format UTC
        for key, value in data.items():
            if isinstance(value, (datetime, timezone.datetime)):
                data[key] = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

        # Supprimer les champs None pour alléger le payload
        data = {k: v for k, v in data.items() if v is not None}

        return data
    # ----------------------------
    # Batch upload queue
    # ----------------------------

    def queue_upload_batch(self, objects, model_name):

        data = [self._serialize_object(o) for o in objects]

        return SyncQueueItem.objects.create(
            model_name=model_name,
            hospital_id=self.hospital_id,
            operation="UPLOAD",
            objects_data=data
        )

    # ----------------------------
    # Batch download queue
    # ----------------------------

    def queue_download_batch(self, objects, model_name):

        return SyncQueueItem.objects.create(
            model_name=model_name,
            hospital_id=self.hospital_id,
            operation="DOWNLOAD",
            objects_data=objects
        )
    def _clean_data_for_create(self, data):
        """
        Nettoyer les données pour création d'un objet,
        compatible UUID + Delta Sync
        """
        # Exclure uniquement les champs internes
        excluded_fields = ['id', 'createdAt', 'updatedAt']
        
        # Tout le reste est conservé, y compris uuid et sync_version
        return {k: v for k, v in data.items() if k not in excluded_fields}


    def _clean_data_for_update(self, data):
        """
        Nettoyer les données pour mise à jour d'un objet,
        compatible UUID + Delta Sync
        """
        # Exclure les champs qui ne doivent pas changer
        excluded_fields = ['id', 'code', 'createdAt', 'updatedAt', 'hospital_id']
        
        # uuid et sync_version restent pour Delta Sync
        return {k: v for k, v in data.items() if k not in excluded_fields}
    
    def _resolve_conflict(self, sync_log, remote_data):
        """Résoudre automatiquement un conflit"""
        if self.config.conflict_strategy == 'REMOTE_WINS':
            # Appliquer les données distantes
            self._apply_remote_data(sync_log, remote_data)
        elif self.config.conflict_strategy == 'LOCAL_WINS':
            # Renvoyer les données locales
            self._resend_local_data(sync_log)
    
    def get_sync_status(self):
        """Obtenir le statut de synchronisation"""
        return {
            'last_upload': self.config.last_sync_upload,
            'last_download': self.config.last_sync_download,
            'pending_uploads': SyncLog.objects.filter(
                hospital_id=self.hospital_id,
                direction='UPLOAD',
                status='PENDING'
            ).count(),
            'pending_downloads': SyncLog.objects.filter(
                hospital_id=self.hospital_id,
                direction='DOWNLOAD',
                status='PENDING'
            ).count(),
            'conflicts': SyncLog.objects.filter(
                hospital_id=self.hospital_id,
                status='CONFLICT'
            ).count(),
        }