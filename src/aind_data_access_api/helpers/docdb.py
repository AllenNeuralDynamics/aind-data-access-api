"""Utilities that go through the MetadataDBClient"""

import logging
from typing import Optional

from aind_data_access_api.document_db import MetadataDbClient


def get_record_by_id(
    client: MetadataDbClient,
    _id: str,
) -> Optional[dict]:
    """Download a record from docdb using the record _id.

    Parameters
    ----------
    client : MetadataDbClient
    _id : str

    Returns
    -------
    Optional[dict]
        _description_
    """
    records = client.retrieve_docdb_records(filter_query={"_id": _id}, limit=1)
    if len(records) > 0:
        return records[0]
    else:
        return None


def get_projection_by_id(
    client: MetadataDbClient,
    _id: str,
    projection: dict,
) -> Optional[dict]:
    """
    Download a record from docdb using the record _id and a projection.

    Projections return fields set to 1 {"field": 1}

    Parameters
    ----------
    client : MetadataDbClient
    _id : str
    projection : dict

    Returns
    -------
    Optional[dict]
        None if record does not exist. Otherwise, it will return the projected
        record as a dict.
    """
    records = client.retrieve_docdb_records(
        filter_query={"_id": _id}, projection=projection, limit=1
    )
    if len(records) > 0:
        return records[0]
    else:
        return None


def get_field_by_id(
    client: MetadataDbClient,
    _id: str,
    field: str,
) -> Optional[dict]:
    """Download a single field from docdb using the record _id

    Parameters
    ----------
    client : MetadataDbClient
    _id : str
    field : str

    Returns
    -------
    Optional[dict]
        None if a record does not exist. Otherwise returns the field in a dict.
    """
    return get_projection_by_id(client, _id=_id, projection={field: 1})


def get_id_from_name(
    client: MetadataDbClient,
    name: str,
) -> str:
    """
    Get the _id of a record in DocDb from its name field. If multiple share
    the same name, only the first record is returned. If no record is found,
    an exception is raised.

    Parameters
    ----------
    client : MetadataDbClient
    name : str

    Returns
    -------
    str
        The _id of the record with the given name.
    """
    records = client.retrieve_docdb_records(
        filter_query={"name": name}, projection={"_id": 1}
    )
    if len(records) > 1:
        logging.warning(
            f"Multiple records share the name {name}, "
            "only the first record will be returned.",
        )
    elif len(records) == 0:
        raise ValueError(f"No record found with name {name}")
    return records[0]["_id"]


def get_name_from_id(
    client: MetadataDbClient,
    _id: str,
) -> str:
    """
    Get the name of a record in DocDb from its _id field. If no record is
    found, an exception is raised.

    Parameters
    ----------
    client : MetadataDbClient
    _id : str

    Returns
    -------
    str
        The name of the record with the given _id.
    """
    records = client.retrieve_docdb_records(
        filter_query={"_id": _id}, projection={"name": 1}, limit=1
    )
    if len(records) == 0:
        raise ValueError(f"No record found with _id {_id}")
    return records[0]["name"]
