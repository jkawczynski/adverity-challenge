from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from .collectors import PeopleCollector
from .models import SwapiCollection
from .reader import get_header, get_value_count, read_collection


def index(request):
    """Index view with Collection listing"""
    collections = SwapiCollection.objects.order_by("-created_at")
    return render(
        request,
        "swapi_collect/index.html",
        {"collections": collections},
    )


def fetch(request):
    """Fetches new collection and redirects to index page"""
    PeopleCollector().collect()
    return redirect("index")


def browse_collection(request, collection_id):
    """View to browse collection data"""
    collection = get_object_or_404(SwapiCollection, pk=collection_id)
    base_context = get_base_context(request, collection)
    limit = int(request.GET.get("limit", 10))
    headers, data = read_collection(collection, limit=limit)
    more_url = f"{request.path}?limit={limit+10}" if limit <= len(data) else None
    return render(
        request,
        "swapi_collect/collection_browse_data.html",
        {
            **base_context,
            "data_headers": headers,
            "data": data,
            "more_url": more_url,
        },
    )


def value_count_collection(request, collection_id):
    """View to browse collection value count data"""
    collection = get_object_or_404(SwapiCollection, pk=collection_id)
    base_context = get_base_context(request, collection)
    columns = get_columns(request, collection)
    selected_columns = [column["name"] for column in columns if column["selected"]]

    data_context = {}
    if selected_columns:
        data_headers, data = get_value_count(collection, selected_columns)
        data_context = {"data_headers": data_headers, "data": data}

    return render(
        request,
        "swapi_collect/collection_value_count.html",
        {**base_context, **data_context, "columns": columns},
    )


def get_collection_menu_items(request, collection_id):
    return [
        {
            "name": "Browse Collection",
            "url": reverse(
                "browse_collection", kwargs={"collection_id": collection_id}
            ),
            "is_active": "browse" in request.path_info,
        },
        {
            "name": "Value Count",
            "url": reverse(
                "value_count_collection", kwargs={"collection_id": collection_id}
            ),
            "is_active": "value_count" in request.path_info,
        },
    ]


def get_base_context(request, collection):
    menu_items = get_collection_menu_items(request, collection.pk)
    return {
        "menu_items": menu_items,
        "collection": collection,
    }


def get_columns(request, collection):
    available_columns = get_header(collection)
    selected_columns = []
    if request.method == "POST":
        selected_columns = request.POST.getlist("columns")

    return [
        {
            "name": column,
            "selected": column in selected_columns,
        }
        for column in available_columns
    ]
