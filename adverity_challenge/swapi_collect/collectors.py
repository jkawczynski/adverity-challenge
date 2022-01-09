import csv
import time
import typing as T
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from dateutil.parser import parse
from django.conf import settings

from .models import SwapiCollection
from .utils import chunked, get_single_page


class SWAPICollectionCollector(ABC):
    """
    Abstract class to collect for collecting data from SWAPI endpoints.

    Collect data from the paginated SWAPI endpoint async way. Using response
    from the first page of the endpoint calculate how many requests are needed
    to fetch all the remaining data. Then create batches of requests using
    BATCH_REQUEST_SIZE settings and request data in batch by batch. 
    Collecting the data should be handled in abstract method `collect_data`.
    """
    endpoint_url: str
    fields_to_exclude = tuple()

    def __init__(self):
        self.session = requests.Session()
        self.generation_time = None

    @abstractmethod
    def collect_data(self, collected_data: T.List[T.Dict]):
        """Abstract method for collecting batch of data"""
        ...

    def custom_transformations(self, row: T.Dict):
        """Perform any custom transformation for signle row of data"""
        pass

    def before_collect(self):
        """Method executed before fetching all the data"""
        pass

    def after_collect(self):
        """Method executed after fetching all the data"""
        pass

    def _transform_single_row(self, row: T.Dict):
        for field_to_exclude in self.fields_to_exclude:
            row.pop(field_to_exclude)

        self.custom_transformations(row)

    def _transform(self, raw_paginated_data: T.Dict[int, T.Dict]):
        transformed_data = []
        for _, page_data in sorted(raw_paginated_data.items()):
            for row in page_data:
                self._transform_single_row(row)
                transformed_data.append(row)

        return transformed_data


    def collect(self):
        """Collects all the data from the endpoint"""
        start = time.time()
        raw_paginated_data = {}
        self.before_collect()
        first_page_response, page_number = get_single_page(
            self.session, self.endpoint_url, page_number=1
        )
        raw_paginated_data[page_number] = first_page_response.results

        with (ThreadPoolExecutor(max_workers=settings.BATCH_REQUEST_SIZE) as executor,):
            futures = (
                executor.submit(
                    get_single_page, self.session, self.endpoint_url, page_number
                )
                for page_number in range(2, first_page_response.last_page + 1)
            )

            for chunk_futures in chunked(futures, settings.BATCH_REQUEST_SIZE):
                for future in as_completed(chunk_futures):
                    page_response, page_number = future.result()
                    raw_paginated_data[page_number] = page_response.results

                transformed_data = self._transform(raw_paginated_data)
                self.collect_data(transformed_data)
                raw_paginated_data = {}

        self.generation_time = round(time.time() - start, 2)
        self.after_collect()


class SWAPICollectionCSVCollector(SWAPICollectionCollector):
    """SWAPI Endpoint collector that collects data straight to the CSV file"""
    delimiter = ","
    fields_to_export = (
        "name",
        "height",
        "mass",
        "hair_color",
        "skin_color",
        "eye_color",
        "birth_year",
        "gender",
        "homeworld",
        "date",
    )
    filename_prefix = ""

    def before_collect(self):
        self.open_and_init_csv_file()

    def collect_data(self, collected_data: T.List[T.Dict]):
        self.append_to_csv(collected_data)

    def after_collect(self):
        self.csv_file.close()

    def open_and_init_csv_file(self):
        file_name = datetime.now().strftime("{self.filename_prefix}%Y-%m-%d-%H-%M-%S.csv")
        self.csv_file = open(settings.COLLECTIONS_DIR / file_name, "w", newline="")
        self.writer = csv.writer(self.csv_file, delimiter=self.delimiter)
        self.writer.writerow(self.fields_to_export)

    def append_to_csv(self, transformed_data: T.List[T.Dict]):
        for transformed_row in transformed_data:
            csv_row = (transformed_row[field] for field in self.fields_to_export)
            self.writer.writerow(csv_row)


class PlanetsNameCollector(SWAPICollectionCollector):
    """
    Collects all the planets and creates a map with url as key and planet name
    as a value
    """
    endpoint_url = f"{settings.SWAPI_URL}/planets"
    fields_to_exclude = (
        "rotation_period",
        "orbital_period",
        "diameter",
        "climate",
        "gravity",
        "terrain",
        "surface_water",
        "population",
        "residents",
        "films",
        "created",
        "edited",
    )

    def before_collect(self):
        self.url_to_name_map = {}

    def collect_data(self, collected_data: T.List[T.Dict]):
        for planet in collected_data:
            self.url_to_name_map[planet["url"]] = planet["name"]


class PeopleCollector(SWAPICollectionCSVCollector):
    """
    Collects all the poeple from the SWAPI
    """
    endpoint_url = f"{settings.SWAPI_URL}/people"
    fields_to_exclude = ("films", "species", "vehicles", "starships", "created", "url")
    filename_prefix = "people_"

    def before_collect(self):
        super().before_collect()
        self.planets = PlanetsNameCollector()
        self.planets.collect()

    def after_collect(self):
        collection = SwapiCollection()
        collection.generation_time = self.generation_time 
        collection.file.name = self.csv_file.name
        collection.save()

    def custom_transformations(self, row: T.Dict):
        row["homeworld"] = self.planets.url_to_name_map[row["homeworld"]]
        row["date"] = parse(row.pop("edited")).strftime("%Y-%m-%d")
