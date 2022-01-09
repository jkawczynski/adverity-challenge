from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("fetch", views.fetch, name="fetch"),
    path(
        "collections/<int:collection_id>/browse",
        views.browse_collection,
        name="browse_collection",
    ),
    path(
        "collections/<int:collection_id>/value_count",
        views.value_count_collection,
        name="value_count_collection",
    ),
]
