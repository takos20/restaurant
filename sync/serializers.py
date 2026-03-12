# sync/serializers.py
from rest_framework import serializers

from hospital.serializers import HospitalSerializer
from .models import SyncLog, SyncConfig


class SyncLogSerializer(serializers.ModelSerializer):
    content_type_name = serializers.CharField(source='content_type.model', read_only=True)
    
    class Meta:
        model = SyncLog
        fields = '__all__'


class SyncConfigSerializer(serializers.ModelSerializer):
    hospital = HospitalSerializer(many=False, fields=('id', 'name'))
    class Meta:
        model = SyncConfig
        fields = '__all__'
        extra_kwargs = {
            'api_token': {'write_only': True}
        }


class SyncRequestSerializer(serializers.Serializer):
    """Serializer pour les requêtes de sync"""
    hospital_id = serializers.IntegerField(required=True)
    force = serializers.BooleanField(default=False)
    models = serializers.ListField(
        child=serializers.CharField(),
        required=False
    )