"""Utilities that go through the MetadataDBClient """

from typing import Optional
from aind_data_access_api.document_db import MetadataDbClient


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


def get_field_from_docdb(
    client: MetadataDbClient,
    _id: str,
    field: str,
) -> Optional[dict]:
    """Download a single field from a docdb record

    Parameters
    ----------
    client : MetadataDbClient
    record_id : str
    field : str

    Returns
    -------
    Optional[dict]
        None if a record does not exist. Otherwise returns the field in a dict.
    """
    return get_projected_record_from_docdb(
        client, record_id=_id, projection={field: 1}
    )


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
