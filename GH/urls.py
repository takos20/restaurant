from GH import settings
from django.contrib import admin
from django.urls import path
from django.conf.urls.static import static
from django.urls import re_path
from django.views.static import serve
from hospital.urls import urlpatterns as hospital_urlpatterns
from restaurants.urls import urlpatterns as restaurant_urlpatterns
from sync.urls import urlpatterns as sync_urlpatterns
# from notification.urls import urlpatterns as notification_urlpatterns
urlpatterns = [
    path('admin/', admin.site.urls),

]
urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
urlpatterns += [
    re_path(r'^media/(?P<path>.*)$', serve, {
        'document_root': settings.MEDIA_ROOT,
    }),
]
urlpatterns += hospital_urlpatterns
urlpatterns += sync_urlpatterns
urlpatterns += restaurant_urlpatterns