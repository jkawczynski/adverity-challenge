import typing as T

import petl as etl

from .models import SwapiCollection

def get_data_from_collection(collection: SwapiCollection):
    """Returns data from SWAPI coleciton"""
    return etl.fromcsv(collection.file.name)

def get_header(collection: SwapiCollection) -> T.Tuple:
    """Get headers for collection"""
    data = get_data_from_collection(collection)
    return etl.header(data)


def read_collection(collection: SwapiCollection, start=0, limit=10) -> T.Tuple:
    """Read collection data in paginated way"""
    data = get_data_from_collection(collection)
    return etl.header(data), data.skip(start + 1).head(limit - 1)


def get_value_count(collection: SwapiCollection, columns: T.List[str]) -> T.Tuple:
    """
    Groups the data from collection for given `columns` and add additional
    value column with number of occurences for such combination
    """
    data = get_data_from_collection(collection)
    aggregated = etl.aggregate(
        data, key=columns[0] if len(columns) == 1 else columns, aggregation=len
    ).sort("value", reverse=True)
    return etl.header(aggregated), aggregated.skip(1)
