from django.contrib import admin
from django.urls import path

from . import views

urlpatterns = [
    path('api/v1/chats/', views.ChatSessionView.as_view()),
    path('api/v1/chats/<uri>/', views.ChatSessionView.as_view()),
    path('api/v1/chats/<uri>/messages/', views.ChatSessionMessageView.as_view()),
]