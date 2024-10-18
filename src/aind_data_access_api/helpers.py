"""Module for convenience functions for the data access API."""
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_schema.core.quality_control import QualityControl
import json


def get_quality_control(client: MetadataDbClient, id: str = "", name: str = ""):
    """Using a connected DocumentDB client, retrieve the QualityControl object for a given record.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    id : str, optional
        _id field in DocDB, by default None
    name : str, optional
        name field in DocDB, by default None
    """
    if id == "" and name == "":
        raise ValueError("Must specify either id or name.")

    if id != "":
        filter_query = {"_id": id}

    if name != "":
        filter_query = {"name": name}

    record = client.retrieve_docdb_records(
        filter_query=filter_query,
        projection={
            "quality_control": 1
        },
        limit=1
    )

    if len(record) == 0:
        raise ValueError(f"No record found with id {id} or name {name}")

    # Try to validate
    qc = QualityControl.model_validate_json(json.dumps(record[0]["quality_control"]))

    return qc
