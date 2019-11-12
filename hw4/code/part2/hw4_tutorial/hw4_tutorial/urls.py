from django.conf.urls import url
from django.urls import path
from . import view

urlpatterns = [
    path('hello', view.hello),
    path('dashboard', view.dashboard),
    path('connection', view.connection)
]