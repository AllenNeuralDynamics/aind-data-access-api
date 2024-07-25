"""Module to interface with the DocumentDB"""

import json
import logging
import warnings
from functools import cached_property
from sys import getsizeof
from typing import List, Optional, Tuple

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from requests import Response

from aind_data_access_api.models import DataAssetRecord
from aind_data_access_api.utils import is_dict_corrupt


class Client:
    """Class to create client to interface with DocumentDB via an API
    Gateway."""

    def __init__(
        self,
        host: str,
        database: str,
        collection: str,
        version: str = "v1",
        boto_session=None,
    ):
        """Class constructor."""
        self.host = host.strip("/")
        self.database = database
        self.collection = collection
        self.version = version
        self._boto_session = boto_session

    @property
    def _base_url(self):
        """Construct base url to interface with a collection in a database."""
        # return helper function
        return self._create_url()

    def _create_url(
        self, database: Optional[str] = None, collection: Optional[str] = None
    ):
        """
        Create url based on input database and collection
        ----------
        database : Optional[str]
          Database of url. Default is None
        collection : Optional[str]
          Collection of url. Default is None

        Returns
        -------
        str
          String of url in
          https://{self.host}/{self.version}/{database}/{collection} format

        """

        database = database if database is not None else self.database
        collection = collection if collection is not None else self.collection

        return (
            f"https://{self.host}/{self.version}/{database}/" f"{collection}"
        )

    @property
    def _update_one_url(self):
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/update_one"
        )

    @property
    def _delete_one_url(self):
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/delete_one"
        )

    @property
    def _delete_many_url(self):
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/delete_many"
        )

    @property
    def _bulk_write_url(self):
        """Url to bulk write many records."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/bulk_write"
        )

    @cached_property
    def __boto_session(self):
        """Boto3 session"""
        if self._boto_session is None:
            self._boto_session = boto3.session.Session()
        return self._boto_session

    def _signed_request(
        self, url: str, method: str, data: Optional[str] = None
    ) -> AWSRequest:
        """Create a signed request to write to the document store.
        Permissions are managed through AWS."""
        aws_request = AWSRequest(
            url=url,
            method=method,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        SigV4Auth(
            self.__boto_session.get_credentials(),
            "execute-api",
            self.__boto_session.region_name,
        ).add_auth(aws_request)
        return aws_request

    def _count_records(
        self,
        database: Optional[str] = None,
        collection: Optional[str] = None,
        filter_query: Optional[dict] = None,
    ):
        """
        Methods to count the number of records in a collection.
        Parameters
        ----------
        database : Optional[str]
          Database of the records being counted. Default is None
        collection : Optional[str]
          Collection of the records being counted. Default is None
        filter_query : Optional[dict]
          If passed, will return the number of records and number of records
          returned by the filter query.

        Returns
        -------
        dict
          Has keys {"total_record_count": int, "filtered_record_count": int}

        """
        params = {
            "count_records": str(True),
        }
        if filter_query is not None:
            params["filter"] = json.dumps(filter_query)
        url = self._create_url(database, collection)
        response = requests.get(url, params=params)
        response_json = response.json()
        body = response_json.get("body")
        return json.loads(body)

    def _get_records(
        self,
        database: Optional[str] = None,
        collection: Optional[str] = None,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: int = 0,
        skip: int = 0,
    ) -> List[dict]:
        """
        Retrieve records from collection.
        Parameters
        ----------
        database : Optional[str]
          Database of the records being returned. Default is None
        collection : Optional[str]
          Collection of the records being returned. Default is None
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[List[Tuple[str, int]]]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        skip : int
          Skip this amount of records in index when applying search.

        Returns
        -------
        List[dict]
          The list of records returned from the API Gateway.

        """
        params = {"limit": str(limit), "skip": str(skip)}
        if filter_query is not None:
            params["filter"] = json.dumps(filter_query)
        if projection is not None:
            params["projection"] = json.dumps(projection)
        if sort is not None:
            params["sort"] = str(sort)

        url = self._create_url(database, collection)
        response = requests.get(url, params=params)
        response_json = response.json()
        body = response_json.get("body")
        if body is None:
            raise KeyError("Body not found in json response")
        else:
            return json.loads(body)

    def _upsert_one_record(
        self, record_filter: dict, update: dict
    ) -> Response:
        """Upsert a single record into the collection."""
        data = json.dumps(
            {"filter": record_filter, "update": update, "upsert": "True"}
        )
        signed_header = self._signed_request(
            method="POST", url=self._update_one_url, data=data
        )
        return requests.post(
            url=self._update_one_url,
            headers=dict(signed_header.headers),
            data=data,
        )

    def _delete_one_record(self, record_filter: dict) -> Response:
        """Upsert a single record into the collection."""
        data = json.dumps({"filter": record_filter})
        signed_header = self._signed_request(
            method="DELETE", url=self._delete_one_url, data=data
        )
        return requests.delete(
            url=self._delete_one_url,
            headers=dict(signed_header.headers),
            data=data,
        )

    def _delete_many_records(self, record_filter: dict) -> Response:
        """Upsert a single record into the collection."""
        data = json.dumps({"filter": record_filter})
        signed_header = self._signed_request(
            method="DELETE", url=self._delete_many_url, data=data
        )
        return requests.delete(
            url=self._delete_many_url,
            headers=dict(signed_header.headers),
            data=data,
        )

    def _bulk_write(self, operations: List[dict]) -> Response:
        """Bulk write many records into the collection."""

        data = json.dumps(operations)
        signed_header = self._signed_request(
            method="POST", url=self._bulk_write_url, data=data
        )
        return requests.post(
            url=self._bulk_write_url,
            headers=dict(signed_header.headers),
            data=data,
        )


class MetadataDbClient(Client):
    """Class to manage reading and writing to metadata db"""

    def retrieve_docdb_records(
        self,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        paginate: bool = True,
        paginate_batch_size: int = 10,
        paginate_max_iterations: int = 20000,
    ) -> List[dict]:
        """
        Retrieve raw json records from DocDB API Gateway as a list of dicts.

        Parameters
        ----------
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        paginate : bool
          If set to true, will batch the queries to the API Gateway. It may
          be faster to set to false if the number of records expected to be
          returned is small.
        paginate_batch_size : int
          Number of records to return at a time. Default is 10.
        paginate_max_iterations : int
          Max number of iterations to run to prevent indefinite calls to the
          API Gateway. Default is 20000.

        Returns
        -------
        List[dict]

        """
        if paginate is False:
            records = self._get_records(
                filter_query=filter_query,
                projection=projection,
                sort=sort,
                limit=limit,
            )
        else:
            # Get record count
            record_counts = self._count_records(filter_query)
            total_record_count = record_counts["total_record_count"]
            filtered_record_count = record_counts["filtered_record_count"]
            if filtered_record_count <= paginate_batch_size:
                records = self._get_records(
                    filter_query=filter_query, projection=projection, sort=sort
                )
            else:
                records = []
                errors = []
                num_of_records_collected = 0
                limit = filtered_record_count if limit == 0 else limit
                skip = 0
                iter_count = 0
                while (
                    skip < total_record_count
                    and num_of_records_collected
                    < min(filtered_record_count, limit)
                    and iter_count < paginate_max_iterations
                ):
                    try:
                        batched_records = self._get_records(
                            filter_query=filter_query,
                            projection=projection,
                            sort=sort,
                            limit=paginate_batch_size,
                            skip=skip,
                        )
                        num_of_records_collected += len(batched_records)
                        records.extend(batched_records)
                    except Exception as e:
                        errors.append(repr(e))
                    skip = skip + paginate_batch_size
                    iter_count += 1
                    # TODO: Add optional progress bar?
                records = records[0:limit]
                if len(errors) > 0:
                    logging.error(
                        f"There were errors retrieving records. {errors}"
                    )
        return records

    def retrieve_schema_records(
        self,
        schema_type: Optional[str] = None,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        paginate: bool = True,
        paginate_batch_size: int = 10,
        paginate_max_iterations: int = 20000,
    ) -> List[dict]:
        """
        Retrieve schemas records from DocDB API Gateway as a list of dicts.

        Parameters
        ----------
        schema_type : Optional[str]
          Type of schema to retrieve. Default is None.
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        paginate : bool
          If set to true, will batch the queries to the API Gateway. It may
          be faster to set to false if the number of records expected to be
          returned is small.
        paginate_batch_size : int
          Number of records to return at a time. Default is 10.
        paginate_max_iterations : int
          Max number of iterations to run to prevent indefinite calls to the
          API Gateway. Default is 20000.

        Returns
        -------
        List[dict]

        """
        if paginate is False:
            records = self._get_records(
                database="schemas",
                collection=schema_type,
                filter_query=filter_query,
                projection=projection,
                sort=sort,
                limit=limit,
            )
        else:
            # Get record count
            record_counts = self._count_records(
                database="schemas",
                collection=schema_type,
                filter_query=filter_query,
            )
            total_record_count = record_counts["total_record_count"]
            filtered_record_count = record_counts["filtered_record_count"]
            if filtered_record_count <= paginate_batch_size:
                records = self._get_records(
                    database="schemas",
                    collection=schema_type,
                    filter_query=filter_query,
                    projection=projection,
                    sort=sort,
                )
            else:
                records = []
                errors = []
                num_of_records_collected = 0
                limit = filtered_record_count if limit == 0 else limit
                skip = 0
                iter_count = 0
                while (
                    skip < total_record_count
                    and num_of_records_collected
                    < min(filtered_record_count, limit)
                    and iter_count < paginate_max_iterations
                ):
                    try:
                        batched_records = self._get_records(
                            database="schemas",
                            collection=schema_type,
                            filter_query=filter_query,
                            projection=projection,
                            sort=sort,
                            limit=paginate_batch_size,
                            skip=skip,
                        )
                        num_of_records_collected += len(batched_records)
                        records.extend(batched_records)
                    except Exception as e:
                        errors.append(repr(e))
                    skip = skip + paginate_batch_size
                    iter_count += 1
                    # TODO: Add optional progress bar?
                records = records[0:limit]
                if len(errors) > 0:
                    logging.error(
                        f"There were errors retrieving records. {errors}"
                    )
        return records

    # TODO: remove this method
    def retrieve_data_asset_records(
        self,
        # add schmema type, string
        # add collection
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        paginate: bool = True,
        paginate_batch_size: int = 10,
        paginate_max_iterations: int = 20000,
    ) -> List[DataAssetRecord]:
        """
        DEPRECATED: This method is deprecated. Use `retrieve_docdb_records`
        instead.

        Retrieve data asset records

        Parameters
        ----------
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        paginate : bool
          If set to true, will batch the queries to the API Gateway. It may
          be faster to set to false if the number of records expected to be
          returned is small.
        paginate_batch_size : int
          Number of records to return at a time. Default is 10.
        paginate_max_iterations : int
          Max number of iterations to run to prevent indefinite calls to the
          API Gateway. Default is 20000.

        Returns
        -------
        List[DataAssetRecord]

        """
        warnings.warn(
            "retrieve_data_asset_records is deprecated. "
            "Use retrieve_docdb_records instead."
            "",
            DeprecationWarning,
            stacklevel=2,
        )
        if paginate is False:
            # and add to count records
            records = self._get_records(
                filter_query=filter_query,
                projection=projection,
                sort=sort,
                limit=limit,
            )
        else:
            # Get record count
            record_counts = self._count_records(filter_query)
            total_record_count = record_counts["total_record_count"]
            filtered_record_count = record_counts["filtered_record_count"]
            if filtered_record_count <= paginate_batch_size:
                records = self._get_records(
                    filter_query=filter_query, projection=projection, sort=sort
                )
            else:
                records = []
                errors = []
                num_of_records_collected = 0
                limit = filtered_record_count if limit == 0 else limit
                skip = 0
                iter_count = 0
                while (
                    skip < total_record_count
                    and num_of_records_collected
                    < min(filtered_record_count, limit)
                    and iter_count < paginate_max_iterations
                ):
                    try:
                        batched_records = self._get_records(
                            filter_query=filter_query,
                            projection=projection,
                            sort=sort,
                            limit=paginate_batch_size,
                            skip=skip,
                        )
                        num_of_records_collected += len(batched_records)
                        records.extend(batched_records)
                    except Exception as e:
                        errors.append(repr(e))
                    skip = skip + paginate_batch_size
                    iter_count += 1
                    # TODO: Add optional progress bar?
                records = records[0:limit]
                if len(errors) > 0:
                    logging.error(
                        f"There were errors retrieving records. {errors}"
                    )
        data_asset_records = []
        for record in records:
            data_asset_records.append(DataAssetRecord(**record))
        return data_asset_records

    def upsert_one_docdb_record(self, record: dict) -> Response:
        """Upsert one record if the record is not corrupt"""
        if record.get("_id") is None:
            raise ValueError("Record does not have an _id field.")
        if is_dict_corrupt(record):
            raise ValueError("Record is corrupt and cannot be upserted.")
        response = self._upsert_one_record(
            record_filter={"_id": record["_id"]},
            update={"$set": json.loads(json.dumps(record, default=str))},
        )
        return response

    # TODO: remove this method
    def upsert_one_record(
        self, data_asset_record: DataAssetRecord
    ) -> Response:
        """
        DEPRECATED: This method is deprecated. Use `upsert_one_docdb_record`
        instead.

        Upsert one record
        """
        warnings.warn(
            "upsert_one_record is deprecated. "
            "Use upsert_one_docdb_record instead."
            "",
            DeprecationWarning,
            stacklevel=2,
        )
        response = self._upsert_one_record(
            record_filter={"_id": data_asset_record.id},
            update={
                "$set": json.loads(
                    data_asset_record.model_dump_json(by_alias=True)
                )
            },
        )
        return response

    def delete_one_record(self, data_asset_record_id: str) -> Response:
        """Delete one record by id"""

        response = self._delete_one_record(
            record_filter={"_id": data_asset_record_id},
        )
        return response

    def delete_many_records(
        self, data_asset_record_ids: List[str]
    ) -> Response:
        """Delete many records by their ids"""

        response = self._delete_many_records(
            record_filter={"_id": {"$in": data_asset_record_ids}},
        )
        return response

    @staticmethod
    def _record_to_operation(record: str, record_id: str) -> dict:
        """Maps a record into an operation"""
        return {
            "UpdateOne": {
                "filter": {"_id": record_id},
                "update": {"$set": json.loads(record)},
                "upsert": "True",
            }
        }

    def upsert_list_of_docdb_records(
        self,
        records: List[dict],
        max_payload_size: int = 2e6,
    ) -> List[Response]:
        """
        Upsert a list of records. There's a limit to the size of the
        request that can be sent, so we chunk the requests.

        Parameters
        ----------

        records : List[dict]
          List of records to upsert into the DocDB database
        max_payload_size : int
          Chunk requests into smaller lists no bigger than this value in bytes.
          If a single record is larger than this value in bytes, an attempt
          will be made to upsert the record but will most likely receive a 413
          status code. The Default is 2e6 bytes. The max payload for the API
          Gateway including headers is 10MB.

        Returns
        -------
        List[Response]
          A list of responses from the API Gateway.

        """
        if len(records) == 0:
            return []
        else:
            # check no record is corrupt or missing _id
            for record in records:
                if record.get("_id") is None:
                    raise ValueError("A record does not have an _id field.")
                if is_dict_corrupt(record):
                    raise ValueError(
                        "A record is corrupt and cannot be upserted."
                    )
            # chunk records
            first_index = 0
            end_index = len(records)
            second_index = 1
            responses = []
            record_json = json.dumps(records[first_index], default=str)
            total_size = getsizeof(record_json)
            operations = [
                self._record_to_operation(
                    record=record_json,
                    record_id=records[first_index].get("_id"),
                )
            ]
            while second_index < end_index + 1:
                if second_index == end_index:
                    response = self._bulk_write(operations)
                    responses.append(response)
                else:
                    record_json = json.dumps(
                        records[second_index], default=str
                    )
                    record_size = getsizeof(record_json)
                    if total_size + record_size > max_payload_size:
                        response = self._bulk_write(operations)
                        responses.append(response)
                        first_index = second_index
                        operations = [
                            self._record_to_operation(
                                record=record_json,
                                record_id=records[first_index].get("_id"),
                            )
                        ]
                        total_size = record_size
                    else:
                        operations.append(
                            self._record_to_operation(
                                record=record_json,
                                record_id=records[second_index].get("_id"),
                            )
                        )
                        total_size += record_size
                second_index = second_index + 1
        return responses

    # TODO: remove this method
    def upsert_list_of_records(
        self,
        data_asset_records: List[DataAssetRecord],
        max_payload_size: int = 2e6,
    ) -> List[Response]:
        """
        DEPRECATED: This method is deprecated. Use
        `upsert_list_of_docdb_records` instead.

        Upsert a list of records. There's a limit to the size of the
        request that can be sent, so we chunk the requests.

        Parameters
        ----------

        data_asset_records : List[DataAssetRecord]
          List of records to upsert into the DocDB database
        max_payload_size : int
          Chunk requests into smaller lists no bigger than this value in bytes.
          If a single record is larger than this value in bytes, an attempt
          will be made to upsert the record but will most likely receive a 413
          status code. The Default is 2e6 bytes. The max payload for the API
          Gateway including headers is 10MB.

        Returns
        -------
        List[Response]
          A list of responses from the API Gateway.

        """
        warnings.warn(
            "upsert_list_of_records is deprecated. "
            "Use upsert_list_of_docdb_records instead."
            "",
            DeprecationWarning,
            stacklevel=2,
        )
        if len(data_asset_records) == 0:
            return []
        else:
            first_index = 0
            end_index = len(data_asset_records)
            second_index = 1
            responses = []
            record_json = data_asset_records[first_index].model_dump_json(
                by_alias=True
            )
            total_size = getsizeof(record_json)
            operations = [
                self._record_to_operation(
                    record=record_json,
                    record_id=data_asset_records[first_index].id,
                )
            ]
            while second_index < end_index + 1:
                if second_index == end_index:
                    response = self._bulk_write(operations)
                    responses.append(response)
                else:
                    record_json = data_asset_records[
                        second_index
                    ].model_dump_json(by_alias=True)
                    record_size = getsizeof(record_json)
                    if total_size + record_size > max_payload_size:
                        response = self._bulk_write(operations)
                        responses.append(response)
                        first_index = second_index
                        operations = [
                            self._record_to_operation(
                                record=record_json,
                                record_id=data_asset_records[first_index].id,
                            )
                        ]
                        total_size = record_size
                    else:
                        operations.append(
                            self._record_to_operation(
                                record=record_json,
                                record_id=data_asset_records[second_index].id,
                            )
                        )
                        total_size += record_size
                second_index = second_index + 1
        return responses
