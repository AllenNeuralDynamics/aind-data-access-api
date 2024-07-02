"""Package for common methods used for interfacing with DocDB."""

from typing import Dict, Iterator, List, Optional
from urllib.parse import urlparse

from pymongo import MongoClient


def get_s3_bucket_and_prefix(s3_location: str) -> Dict[str, str]:
    """
    For a location url like s3://bucket/prefix, it will return the bucket
    and prefix. It doesn't check the scheme is s3. It will strip the leading
    and trailing forward slashes from the prefix.
    Parameters
    ----------
    s3_location : str
      For example, 's3://some_bucket/some_prefix'

    Returns
    -------
    Dict[str, str]
      For example, {'bucket': 'some_bucket', 'prefix': 'some_prefix'}

    """
    parts = urlparse(s3_location, allow_fragments=False)
    stripped_prefix = parts.path.strip("/")
    return {"bucket": parts.netloc, "prefix": stripped_prefix}


def get_s3_location(bucket: str, prefix: str) -> str:
    """
    For a given bucket and prefix, return a location url in format
    s3://{bucket}/{prefix}
    Parameters
    ----------
    bucket : str
    prefix : str

    Returns
    -------
    str
      For example, 's3://some_bucket/some_prefix'

    """
    stripped_prefix = prefix.strip("/")
    return f"s3://{bucket}/{stripped_prefix}"


def is_dict_corrupt(input_dict: dict) -> bool:
    """
    Checks that all the keys, included nested keys, don't contain '$' or '.'
    Parameters
    ----------
    input_dict : dict

    Returns
    -------
    bool
      True if input_dict is not a dict, or if nested dictionary keys contain
      forbidden characters. False otherwise.

    """
    if not isinstance(input_dict, dict):
        return True
    for key, value in input_dict.items():
        if "$" in key or "." in key:
            return True
        elif isinstance(value, dict):
            if is_dict_corrupt(value):
                return True
    return False


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
    docdb_client: MongoClient,
    db_name: str,
    collection_name: str,
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
    db = docdb_client[db_name]
    collection = db[collection_name]
    records = list(collection.find(filter={"_id": record_id}, limit=1))
    if len(records) > 0:
        return records[0]
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
