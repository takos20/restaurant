from django.urls import path
from django.conf.urls import include
from rest_framework import routers
from sync.views import SyncLogViewSet, SyncViewSet
router = routers.DefaultRouter(trailing_slash=False)
router.register(r'sync', SyncViewSet, basename='sync')
router.register(r'sync_logs', SyncLogViewSet, basename='sync_logs')
urlpatterns = [
    path('api/v1/', include(router.urls)),
]
