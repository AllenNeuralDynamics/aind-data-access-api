"""Package for common methods used for interfacing with DocDB."""

from typing import Dict, Iterator, List, Optional
from urllib.parse import urlparse

from aind_data_access_api.document_db import MetadataDbClient


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


def does_metadata_record_exist_in_docdb(
    docdb_api_client: MetadataDbClient,
    bucket: str,
    prefix: str,
) -> bool:
    """
    For a given bucket and prefix, check if there is already a record in DocDb

    Parameters
    ----------
    docdb_api_client : MetadataDbClient
    bucket : str
    prefix : str

    Returns
    -------
    True if there is a record in DocDb. Otherwise, False.

    """
    location = get_s3_location(bucket=bucket, prefix=prefix)
    records = docdb_api_client.retrieve_docdb_records(
        filter_query={"location": location},
        projection={"_id": 1},
        limit=1,
    )
    if len(records) == 0:
        return False
    else:
        return True


def get_record_from_docdb(
    docdb_api_client: MetadataDbClient,
    record_id: str,
) -> Optional[dict]:
    """
    Download a record from docdb using the record _id.

    Parameters
    ----------
    docdb_api_client : MetadataDbClient
    record_id : str

    Returns
    -------
    Optional[dict]
        None if record does not exist. Otherwise, it will return the record as
        a dict.

    """
    records = docdb_api_client.retrieve_docdb_records(
        filter_query={"_id": record_id}, limit=1
    )
    if len(records) > 0:
        return records[0]
    else:
        return None


def paginate_docdb(
    docdb_api_client: MetadataDbClient,
    page_size: int = 500,
    filter_query: Optional[dict] = None,
    projection: Optional[dict] = None,
) -> Iterator[List[dict]]:
    """
    Paginate through records in DocDb.

    Parameters
    ----------
    docdb_api_client : MetadataDbClient
    page_size : int
      Default is 500
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
    skip = 0
    while True:
        page = docdb_api_client._find_records(
            filter_query=filter_query,
            projection=projection,
            limit=page_size,
            skip=skip,
        )
        if not page:
            break
        yield page
        skip += len(page)


def fetch_records_by_filter_list(
    docdb_api_client: MetadataDbClient,
    filter_key: str,
    filter_values: List[str],
    projection: Optional[dict] = None,
) -> List[dict]:
    """
    Queries DocDB for records where the value of a specified field is in a
    list of values. Uses an aggregation pipeline with $in filter operator.

    Parameters
    ----------
    docdb_api_client : MetadataDbClient
    filter_key : str
      The field to filter on.
    filter_values : List[str]
      The list of values to filter on.
    projection : Optional[dict]
      Subset of fields to return. Default is None which returns all fields.

    Returns
    -------
    List[dict]
    """
    agg_pipeline = [{"$match": {filter_key: {"$in": filter_values}}}]
    if projection:
        agg_pipeline.append({"$project": projection})
    results = docdb_api_client.aggregate_docdb_records(pipeline=agg_pipeline)
    return results


def build_docdb_location_to_id_map(
    docdb_api_client: MetadataDbClient,
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
    docdb_api_client : MetadataDbClient
    bucket : str
    prefixes : List[str]

    Returns
    -------
    Dict[str, str]

    """
    locations = [get_s3_location(bucket=bucket, prefix=p) for p in prefixes]
    projection = {"_id": 1, "location": 1}
    results = fetch_records_by_filter_list(
        docdb_api_client=docdb_api_client,
        filter_key="location",
        filter_values=locations,
        projection=projection,
    )
    location_to_id_map = {r["location"]: r["_id"] for r in results}
    return location_to_id_map
