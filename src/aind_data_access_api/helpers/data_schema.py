"""Module for convenience functions for the data access API."""

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.helpers.docdb import (
    get_field_by_id,
    get_id_from_name,
)
from aind_data_schema.core.quality_control import QualityControl
import json


def get_quality_control_by_id(
    client: MetadataDbClient,
    _id: str,
    allow_invalid: bool = False,
):
    """Using a connected DocumentDB client, retrieve the QualityControl object
    for a given record.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    _id : str, optional
        _id field in DocDB, by default empty
    allow_invalid : bool, optional
        return invalid QualityControl as dict if True, by default False
    """
    record = get_field_by_id(client, _id=_id, field="quality_control")
    if not record:
        raise ValueError(f"No record found with id {_id}")

    if "quality_control" not in record or not record["quality_control"]:
        raise ValueError(
            f"No quality_control field found in record with id {_id}"
        )

    return validate_qc(record["quality_control"], allow_invalid=allow_invalid)


def get_quality_control_by_name(
    client: MetadataDbClient,
    name: str,
    allow_invalid: bool = False,
):
    """Using a connected DocumentDB client, retrieve the QualityControl object
    for a given record.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    name : str, optional
        name field in DocDB, by default empty
    allow_invalid : bool, optional
        return invalid QualityControl as dict if True, by default False
    """
    _id = get_id_from_name(client, name=name)
    if not _id:
        raise ValueError(f"No record found with name {name}")

    return get_quality_control_by_id(
        client, _id=_id, allow_invalid=allow_invalid
    )


def validate_qc(qc_data: dict, allow_invalid: bool = False):
    """Validate a quality control dict."""

    try:
        return QualityControl.model_validate_json(json.dumps(qc_data))
    except Exception as e:
        if allow_invalid:
            return qc_data
        else:
            raise e
