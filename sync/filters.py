import django_filters

from sync.models import SyncConfig, SyncLog
from django.utils import timezone
from datetime import datetime, time

class SyncConfigFilter(django_filters.FilterSet):
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    

    class Meta:
        model = SyncConfig
        fields = {'id': ['exact']}

class SyncLogFilter(django_filters.FilterSet):
    hospital = django_filters.CharFilter(field_name='hospital__id', lookup_expr='exact')
    content_type = django_filters.CharFilter(field_name='content_type__id', lookup_expr='exact')
    createdAt = django_filters.DateFilter(lookup_expr='exact')
    start_date = django_filters.DateFilter(method='filter_start_date')
    end_date = django_filters.DateFilter(method='filter_end_date')

    def filter_start_date(self, queryset, name, value):
        start = timezone.make_aware(
            datetime.combine(value, time.min)
        )
        return queryset.filter(createdAt__gte=start)

    def filter_end_date(self, queryset, name, value):
        end = timezone.make_aware(
            datetime.combine(value, time.max)
        )
        return queryset.filter(createdAt__lte=end)
    

    class Meta:
        model = SyncLog
        fields = {'id': ['exact']}
