from django.urls import path, include

urlpatterns = [
        path("", include("swapi_collect.urls")),
]
