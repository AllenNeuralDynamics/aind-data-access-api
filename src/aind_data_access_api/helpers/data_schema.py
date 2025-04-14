"""Module for convenience functions for the data access API."""

import json
import pandas as pd
from typing import List

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


def get_quality_control_df(
    client: MetadataDbClient,
    ids: List[str],
):
    """Using a connected DocumentDB client, retrieve a valid QC object as a dataframe

    Parameters
    ----------
    client : MetadataDbClient
        A connected DocumentDB client.
    _id : str, optional
        _id field in DocDB, by default empty
    allow_invalid : bool, optional
        return invalid QualityControl as dict if True, by default False
    """
    qcs = [
        get_quality_control_by_id(client, _id=_id, allow_invalid=False)
        for _id in ids
    ]

    data = []

    for _id, qc in zip(ids, qcs):
        qc_metrics_flat = {}
        qc_metrics_flat["_id"] = _id
        for eval in qc.evaluations:
            for metric in eval.metrics:
                qc_metrics_flat[f"{eval.name}_{metric.name}.value"] = (
                    metric.value
                )
                qc_metrics_flat[f"{eval.name}_{metric.name}.status"] = (
                    metric.status.status
                )

        data.append(qc_metrics_flat)

    return pd.DataFrame(data)
