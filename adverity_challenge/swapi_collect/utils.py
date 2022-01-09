import typing as T
from dataclasses import dataclass
from itertools import islice
from math import ceil

import requests


def chunked(iterable, batch_size):
    """Chunk the iterable into smaller batches"""
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch

@dataclass
class SwapiPaginatedResponse:
    """
    Dataclass for SWAPI paginated response with additional helper properties
    """
    count: int
    results: T.List[T.Dict]

    @property
    def page_size(self) -> int:
        return len(self.results)

    @property
    def last_page(self) -> int:
        return ceil(self.count / self.page_size)


def serialize_response(response: requests.Response) -> SwapiPaginatedResponse:
    """Serialize SWAPI response"""
    json_response = response.json()

    return SwapiPaginatedResponse(
        count=json_response["count"],
        results=json_response["results"],
    )


def get_single_page(
    session: requests.Session, url: str, page_number: int
) -> T.Tuple[SwapiPaginatedResponse, int]:
    """Get single page of SWAPI endpoint"""
    return (
        serialize_response(session.get(url, params={"page": page_number})),
        page_number,
    )

