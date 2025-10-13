"""Module for convenience functions for the data access API."""

import json
from datetime import datetime, timezone
from typing import List, Optional, Union

import pandas as pd
from aind_data_schema.core.quality_control import QualityControl

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.helpers.docdb import (
    get_field_by_id,
    get_id_from_name,
)


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


def get_quality_control_by_names(
    client: MetadataDbClient,
    names: List[str],
    allow_invalid: bool = False,
) -> List[Union[QualityControl, dict]]:
    """Using a connected DocumentDB client, retrieve the QualityControl object
    for a list of records.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    names : List[str],
        name fields in DocDB
    allow_invalid : bool, optional
        return invalid QualityControl as dict if True, by default False
    """
    records = client.fetch_records_by_filter_list(
        filter_key="name",
        filter_values=names,
        projection={"quality_control": 1},
    )

    qcs = [
        validate_qc(record["quality_control"], allow_invalid=allow_invalid)
        for record in records
    ]

    return qcs


def get_quality_control_status_df(
    client: MetadataDbClient,
    names: List[str],
    date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Using a connected DocumentDB client, retrieve the status of all
    QualityControl objects for a list of records.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    names : List[str],
        name fields in DocDB
    """
    if date is None:
        date = datetime.now(tz=timezone.utc)

    qcs = get_quality_control_by_names(
        client=client, names=names, allow_invalid=False
    )

    data = []

    for name, qc in zip(names, qcs):
        qc_metrics_flat = {}
        qc_metrics_flat["name"] = name
        for metric in qc.metrics:
            # Find the most recent status before the given datetime
            for status in reversed(metric.status_history):
                if status.timestamp <= date:
                    qc_metrics_flat[metric.name] = status.status
                    break

        data.append(qc_metrics_flat)

    return pd.DataFrame(data)


def get_quality_control_value_df(
    client: MetadataDbClient,
    names: List[str],
) -> pd.DataFrame:
    """Using a connected DocumentDB client, retrieve the value of all
    QualityControl objects for a list of records.

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    names : List[str],
        name fields in DocDB
    """
    qcs = get_quality_control_by_names(
        client=client, names=names, allow_invalid=False
    )

    data = []

    for name, qc in zip(names, qcs):
        qc_metrics_flat = {}
        qc_metrics_flat["name"] = name
        for metric in qc.metrics:
            qc_metrics_flat[metric.name] = metric.value

        data.append(qc_metrics_flat)

    return pd.DataFrame(data)
