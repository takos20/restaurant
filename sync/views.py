# sync/views.py
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.utils import timezone
from django_filters.rest_framework import DjangoFilterBackend
from globals.pagination import CustomPagination
from hospital.helpers import SyncPermission
from sync.filters import SyncConfigFilter, SyncLogFilter
from sync.models import SyncConfig, SyncLog
from .sync_service import SyncService
from .serializers import SyncLogSerializer, SyncConfigSerializer
from rest_framework.permissions import AllowAny, IsAuthenticated, DjangoModelPermissions
from rest_framework.renderers import JSONRenderer
from django_filters import rest_framework as filters
from django.apps import apps
from rest_framework.response import Response
from django.utils.dateparse import parse_datetime
from django.core.paginator import Paginator
import logging

class SyncViewSet(viewsets.ViewSet):
    authentication_classes = []
    permission_classes = [AllowAny]
    filter_backends = [DjangoFilterBackend]

    def get_permissions(self):
        return [AllowAny()]

    @action(detail=False, methods=["post"],url_path=r'model/(?P<models>[^/.]+)', url_name='sync-model-create')
    def sync_model_create(self, request, models=None):
        api_key = request.headers.get("X-API-KEY")
        hospital_id = request.data.get("hospital_id")
        uuid_val = request.data.get("uuid")  # UUID du client
        sync_version = request.data.get("sync_version", 1)

        if not hospital_id:
            return Response({"error": "hospital_id requis"}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)

            # Vérification API KEY
            if not api_key or api_key != config.api_token:
                return Response({"error": "Unauthorized"}, status=401)

            # Vérifier si le modèle est autorisé
            model_name = models.capitalize()
            model_path = next(
                (m for m in config.models_to_sync if m.endswith(f".{model_name}")),
                None
            )
            if not model_path:
                return Response({"error": "Model non autorisé"}, status=404)

            app_label, model_cls = model_path.split(".")
            Model = apps.get_model(app_label, model_cls)

        except SyncConfig.DoesNotExist:
            return Response({"error": "SyncConfig introuvable"}, status=404)

        if not uuid_val:
            logging.getLogger('errors_file').info(msg={"error": f"Objet sans uuid: {request.data}"})
            return Response({"error": "uuid requis"}, status=400)

        # Vérifier si l'objet existe déjà par UUID
        local_obj = Model.objects.filter(uuid=uuid_val).first()
        operation = 'UPDATE' if local_obj else 'CREATE'

        try:
            service = SyncService(hospital_id=hospital_id, config=config)
            clean_data = None

            if operation == 'CREATE':
                clean_data = service._clean_data_for_create(request.data)
                # Ajouter UUID et sync_version pour Delta Sync
                clean_data['uuid'] = uuid_val
                clean_data['sync_version'] = sync_version
                if hasattr(Model, 'hospital_id'):
                    clean_data['hospital_id'] = hospital_id
                local_obj = Model.objects.create(**clean_data)
            else:
                # Mise à jour seulement si la version distante est plus récente
                if sync_version > getattr(local_obj, 'sync_version', 0):
                    clean_data = service._clean_data_for_update(request.data)
                    for key, value in clean_data.items():
                        setattr(local_obj, key, value)
                    local_obj.sync_version = sync_version
                    local_obj.save()
                else:
                    # Version locale plus récente → ignorer
                    return Response({
                        "message": "Objet ignoré, version locale plus récente",
                        "uuid": uuid_val
                    }, status=200)

            return Response({'message': 'Upload terminé', 'results': clean_data}, status=201)

        except Exception as e:
            logging.getLogger('errors_file').info(msg={"error": str(e)})
            return Response({'error': str(e)}, status=500)
    @action(
        detail=False,
        methods=["get"],
        url_path=r'model/(?P<models>[^/.]+)',
        url_name='sync-model-list'
    )
    def sync_model_list(self, request, models=None):
        hospital_id = request.query_params.get("hospital_id")
        last_version = int(request.query_params.get("since_version", 0))
        page_number = int(request.query_params.get("page", 1))
        limit = int(request.query_params.get("limit", 500))

        api_key = request.headers.get("X-API-KEY")

        if not hospital_id:
            return Response({"error": "hospital_id requis"}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)

            # Vérification API KEY
            if not api_key or api_key != config.api_token:
                return Response({"error": "Unauthorized"}, status=401)

            # Vérifier si le modèle est autorisé
            model_name = models.capitalize()
            model_path = next(
                (m for m in config.models_to_sync if m.endswith(f".{model_name}")),
                None
            )
            if not model_path:
                return Response({"error": "Model non autorisé"}, status=404)

            app_label, model_cls = model_path.split(".")
            Model = apps.get_model(app_label, model_cls)

        except SyncConfig.DoesNotExist:
            return Response({"error": "SyncConfig introuvable"}, status=404)

        # =========================
        # FILTRAGE pour Delta Sync
        # =========================
        queryset = Model.objects.filter(deleted=False)
        if hasattr(Model, "hospital_id"):
            queryset = queryset.filter(hospital_id=hospital_id)

        if last_version:
            if hasattr(Model, "sync_version"):
                queryset = queryset.filter(sync_version__gt=last_version)
            else:
                # fallback sur updatedAt si pas de sync_version
                updated_since = request.query_params.get("updated_since")
                if updated_since:
                    parsed_date = parse_datetime(updated_since)
                    if parsed_date:
                        queryset = queryset.filter(updatedAt__gt=parsed_date)

        queryset = queryset.order_by("updatedAt")

        # =========================
        # PAGINATION
        # =========================
        paginator = Paginator(queryset, limit)
        page = paginator.get_page(page_number)

        # Sérialiser les objets pour inclure UUID + sync_version
        service = SyncService(hospital_id=hospital_id, config=config)
        data = [service._serialize_object(obj) for obj in page.object_list]

        return Response({
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "current_page": page.number,
            "has_next": page.has_next(),
            "has_previous": page.has_previous(),
            "results": data
        })
    

    @action(
        detail=False,
        methods=["get"],
        url_path=r'model/(?P<models>[^/.]+)',
        url_name='sync-model-batch-list'
    )
    def sync_model_batch_list(self, request, models=None):
        hospital_id = request.query_params.get("hospital_id")
        last_version = int(request.query_params.get("since_version", 0))
        limit = int(request.query_params.get("limit", 1000))  # batch large par défaut
        api_key = request.headers.get("X-API-KEY")

        if not hospital_id:
            return Response({"error": "hospital_id requis"}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)
            if not api_key or api_key != config.api_token:
                return Response({"error": "Unauthorized"}, status=401)

            model_name = models.capitalize()
            model_path = next(
                (m for m in config.models_to_sync if m.endswith(f".{model_name}")),
                None
            )
            if not model_path:
                return Response({"error": "Model non autorisé"}, status=404)

            app_label, model_cls = model_path.split(".")
            Model = apps.get_model(app_label, model_cls)

        except SyncConfig.DoesNotExist:
            return Response({"error": "SyncConfig introuvable"}, status=404)

        # =========================
        # FILTRAGE
        # =========================
        queryset = Model.objects.filter(deleted=False)
        if hasattr(Model, "hospital_id"):
            queryset = queryset.filter(hospital_id=hospital_id)

        if last_version and hasattr(Model, "sync_version"):
            queryset = queryset.filter(sync_version__gt=last_version)

        queryset = queryset.order_by("updatedAt")[:limit]  # limite pour batch

        # Sérialiser les objets
        service = SyncService(hospital_id=hospital_id, config=config)
        data = [service._serialize_object(obj) for obj in queryset]

        return Response({"results": data, "count": len(data)}, status=200)

    @action(
        detail=False,
        methods=["post"],
        url_path=r'model/(?P<models>[^/.]+)/batch',
        url_name='sync-model-batch-create'
    )
    def sync_model_batch_create(self, request, models=None):
        api_key = request.headers.get("X-API-KEY")
        hospital_id = request.data.get("hospital_id")
        objects_data = request.data.get("objects", [])

        if not hospital_id:
            return Response({"error": "hospital_id requis"}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)

            # Vérification API KEY
            if not api_key or api_key != config.api_token:
                return Response({"error": "Unauthorized"}, status=401)

            # Vérifier si le modèle est autorisé
            model_name = models.capitalize()
            model_path = next(
                (m for m in config.models_to_sync if m.endswith(f".{model_name}")),
                None
            )
            if not model_path:
                return Response({"error": "Model non autorisé"}, status=404)

            app_label, model_cls = model_path.split(".")
            Model = apps.get_model(app_label, model_cls)

        except SyncConfig.DoesNotExist:
            return Response({"error": "SyncConfig introuvable"}, status=404)

        if not objects_data:
            return Response({"error": "Aucun objet à synchroniser"}, status=400)

        service = SyncService(hospital_id=hospital_id, config=config)
        results = {"success": 0, "skipped": 0, "failed": 0}

        for obj_data in objects_data:
            uuid_val = obj_data.get("uuid")
            sync_version = obj_data.get("sync_version", 1)

            if not uuid_val:
                results["failed"] += 1
                logging.getLogger('errors_file').info(msg={"error": f"Objet sans UUID: {obj_data}"})
                continue

            # Vérifier si l'objet existe déjà
            local_obj = Model.objects.filter(uuid=uuid_val).first()
            operation = "UPDATE" if local_obj else "CREATE"

            try:
                if operation == "CREATE":
                    clean_data = service._clean_data_for_create(obj_data)
                    clean_data["uuid"] = uuid_val
                    clean_data["sync_version"] = sync_version
                    if hasattr(Model, "hospital_id"):
                        clean_data["hospital_id"] = hospital_id
                    Model.objects.create(**clean_data)
                    results["success"] += 1
                else:
                    # Update seulement si la version distante est plus récente
                    if sync_version > getattr(local_obj, "sync_version", 0):
                        clean_data = service._clean_data_for_update(obj_data)
                        for key, value in clean_data.items():
                            setattr(local_obj, key, value)
                        local_obj.sync_version = sync_version
                        local_obj.save()
                        results["success"] += 1
                    else:
                        results["skipped"] += 1

            except Exception as e:
                logging.getLogger('errors_file').info(msg={"error": str(e), "data": obj_data})
                results["failed"] += 1

        return Response({"message": "Batch upload terminé", "results": results}, status=201)
    @action(detail=False, methods=['post'], url_path='download/(?P<hospital_id>[^/.]+)')
    def download(self, request, hospital_id=None):
        force = request.data.get('force', False)

        if not hospital_id:
            return Response({'error': 'hospital_id requis'}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)

        except SyncConfig.DoesNotExist:
            return Response(
                {'error': f'Aucune configuration de synchronisation pour hospital {hospital_id}'},
                status=404
            )

        try:
            service = SyncService(hospital_id=hospital_id, config=config)

            results = service.download_changes(force=force)

            return Response({
                'message': 'Download terminé',
                'hospital_id': hospital_id,
                'results': results
            })

        except Exception as e:
            logging.getLogger('errors_file').error(
                {"error": f"Erreur sync download hospital {hospital_id}: {str(e)}"}
            )

            return Response(
                {'error': 'Erreur lors de la synchronisation', 'details': str(e)},
                status=500
            )

    @action(detail=False, methods=['post'], url_path='full_sync/(?P<hospital_id>[^/.]+)')
    def full_sync(self, request, hospital_id=None):

        if not hospital_id:
            return Response({'error': 'hospital_id requis'}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)
            service = SyncService(hospital_id=hospital_id, config=config)

            upload_results = service.upload_changes(force=True)
            download_results = service.download_changes(force=True)

            return Response({
                'message': 'Sync complète terminée',
                'upload': upload_results,
                'download': download_results,
                'status': service.get_sync_status()
            })

        except Exception as e:
            return Response({'error': str(e)}, status=500)
    
    @action(detail=False, methods=['post'], url_path='upload/(?P<hospital_id>[^/.]+)')
    def upload(self, request, hospital_id=None):
        force = request.data.get('force', False)

        if not hospital_id:
            return Response({'error': 'hospital_id requis'}, status=400)

        try:
            config = SyncConfig.objects.get(hospital_id=hospital_id)
            service = SyncService(hospital_id=hospital_id, config=config)
            results = service.upload_changes(force=force)

            return Response({'message': 'Upload terminé', 'results': results})

        except Exception as e:
            return Response({'error': str(e)}, status=500)

        

class SyncLogViewSet(viewsets.ViewSet):
    queryset = SyncLog.objects.filter(deleted=False)
    serializer_class = SyncLogSerializer
    renderer_classes = [JSONRenderer]
    permission_classes = (IsAuthenticated, DjangoModelPermissions)
    pagination_class = CustomPagination
    filterset_class = SyncLogFilter
    filter_backends = (filters.DjangoFilterBackend,)