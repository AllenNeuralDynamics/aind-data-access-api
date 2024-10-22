"""Module for DocDB utility functions"""


from typing import Dict, Iterator, List, Optional
from pymongo import MongoClient

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.utils_s3 import get_s3_location


def does_metadata_record_exist_in_docdb(
    docdb_client: MongoClient,
    db_name: str,
    collection_name: str,
    bucket: str,
    prefix: str,
) -> bool:
    """
    For a given bucket and prefix, check if there is already a record in DocDb
    Parameters
    ----------
    docdb_client : MongoClient
    db_name : str
    collection_name : str
    bucket : str
    prefix : str

    Returns
    -------
    True if there is a record in DocDb. Otherwise, False.

    """
    location = get_s3_location(bucket=bucket, prefix=prefix)
    db = docdb_client[db_name]
    collection = db[collection_name]
    records = list(
        collection.find(
            filter={"location": location}, projection={"_id": 1}, limit=1
        )
    )
    if len(records) == 0:
        return False
    else:
        return True


def get_record_from_docdb(
    client: MetadataDbClient,
    record_id: str,
) -> Optional[dict]:
    """
    Download a record from docdb using the record _id.
    Parameters
    ----------
    docdb_client : MongoClient
    db_name : str
    collection_name : str
    record_id : str

    Returns
    -------
    Optional[dict]
        None if record does not exist. Otherwise, it will return the record as
        a dict.

    """
    records = client.retrieve_docdb_records(
        filter_query={"_id": record_id}, limit=1
    )
    if len(records) > 0:
        return records[0]
    else:
        return None


def get_projected_record_from_docdb(
    client: MetadataDbClient,
    record_id: str,
    projection: dict,
) -> Optional[dict]:
    """
    Download a record from docdb using the record _id and a projection.

    Parameters
    ----------
    docdb_client : MongoClient
    db_name : str
    collection_name : str
    record_id : str
    projection : dict

    Returns
    -------
    Optional[dict]
        None if record does not exist. Otherwise, it will return the projected
        record as a dict.
    """
    records = client.retrieve_docdb_records(
        filter_query={"_id": record_id}, projection=projection, limit=1
    )
    if len(records) > 0:
        return records[0]
    else:
        return None


def get_id_from_name(
    client: MetadataDbClient,
    name: str,
) -> Optional[str]:
    """
    Get the _id of a record in DocDb using the name field.
    Parameters
    ----------
    docdb_client : MongoClient
    db_name : str
    collection_name : str
    name : str

    Returns
    -------
    Optional[str]
        None if record does not exist. Otherwise, it will return the _id of
        the record.
    """
    records = client.retrieve_docdb_records(
        filter_query={"name": name}, projection={"_id": 1}, limit=1
    )
    if len(records) > 0:
        return records[0]["_id"]
    else:
        return None


def paginate_docdb(
    db_name: str,
    collection_name: str,
    docdb_client: MongoClient,
    page_size: int = 1000,
    filter_query: Optional[dict] = None,
    projection: Optional[dict] = None,
) -> Iterator[List[dict]]:
    """
    Paginate through records in DocDb.
    Parameters
    ----------
    db_name : str
    collection_name : str
    docdb_client : MongoClient
    page_size : int
      Default is 1000
    filter_query : Optional[dict]
    projection : Optional[dict]

    Returns
    -------
    Iterator[List[dict]]

    """
    if filter_query is None:
        filter_query = {}
    if projection is None:
        projection = {}
    db = docdb_client[db_name]
    collection = db[collection_name]
    cursor = collection.find(filter=filter_query, projection=projection)
    obj = next(cursor, None)
    while obj:
        page = []
        while len(page) < page_size and obj:
            page.append(obj)
            obj = next(cursor, None)
        yield page


def build_docdb_location_to_id_map(
    db_name: str,
    collection_name: str,
    docdb_client: MongoClient,
    bucket: str,
    prefixes: List[str],
) -> Dict[str, str]:
    """
    For a given s3 bucket and list of prefixes, return a dictionary that looks
    like {'s3://bucket/prefix': 'abc-1234'} where the value is the id of the
    record in DocDb. If the record does not exist, then there will be no key
    in the dictionary.
    Parameters
    ----------
    db_name : str
    collection_name : ste
    docdb_client : MongoClient
    bucket : str
    prefixes : List[str]

    Returns
    -------
    Dict[str, str]

    """
    locations = [get_s3_location(bucket=bucket, prefix=p) for p in prefixes]
    filter_query = {"location": {"$in": locations}}
    projection = {"_id": 1, "location": 1}
    db = docdb_client[db_name]
    collection = db[collection_name]
    results = collection.find(filter=filter_query, projection=projection)
    location_to_id_map = {r["location"]: r["_id"] for r in results}
    return location_to_id_map
