"""Module for convenience functions for the data access API."""

from typing import Optional
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.utils.docdb import (
    get_projected_record_from_docdb,
    get_id_from_name,
)
from aind_data_schema.core.quality_control import QualityControl
import json


def get_quality_control(
    client: MetadataDbClient,
    _id: Optional[str] = None,
    name: Optional[str] = None,
    allow_invalid: bool = False,
):
    """Using a connected DocumentDB client, retrieve the QualityControl object for a given record.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    id : str, optional
        _id field in DocDB, by default empty
    name : str, optional
        name field in DocDB, by default empty
    allow_invalid : bool, optional
        return invalid QualityControl as dict if True, by default False
    """
    if not _id and not name:
        raise ValueError("Must specify either _id or name.")

    if name:
        _id = get_id_from_name(client, name=name)

    if not _id:
        raise ValueError(f"No record found with name {name}")

    record = get_projected_record_from_docdb(
        client, record_id=_id, projection={"quality_control": 1}
    )
    if not record:
        raise ValueError(f"No record found with id {_id} or name {name}")

    qc_data = record["quality_control"]

    # Try to validate
    try:
        return QualityControl.model_validate_json(json.dumps(qc_data))
    except Exception as e:
        if allow_invalid:
            return qc_data
        else:
            raise e
