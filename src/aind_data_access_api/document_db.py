"""Module to interface with the DocumentDB"""

import json
import logging
from sys import getsizeof
from typing import List, Optional, Tuple

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from requests import Response

from aind_data_access_api.models import DataAssetRecord


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
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}"
        )

    @property
    def _update_one_url(self):
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/update_one"
        )

    @property
    def _bulk_write_url(self):
        """Url to bulk write many records."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/bulk_write"
        )

    @property
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

    def _count_records(self, filter_query: Optional[dict] = None):
        """
        Methods to count the number of records in a collection.
        Parameters
        ----------
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
        response = requests.get(self._base_url, params=params)
        response_json = response.json()
        body = response_json.get("body")
        return json.loads(body)

    def _get_records(
        self,
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

        response = requests.get(self._base_url, params=params)
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

    def retrieve_data_asset_records(
        self,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        paginate: bool = True,
        paginate_batch_size: int = 10,
        paginate_max_iterations: int = 20000,
    ) -> List[DataAssetRecord]:
        """
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
        data_asset_records = []
        for record in records:
            data_asset_records.append(DataAssetRecord(**record))
        return data_asset_records

    def upsert_one_record(
        self, data_asset_record: DataAssetRecord
    ) -> Response:
        """Upsert one record"""

        response = self._upsert_one_record(
            record_filter={"_id": data_asset_record.id},
            update={"$set": json.loads(data_asset_record.json(by_alias=True))},
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

    def upsert_list_of_records(
        self,
        data_asset_records: List[DataAssetRecord],
        max_payload_size: int = 2e6,
    ) -> List[Response]:
        """
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
        if len(data_asset_records) == 0:
            return []
        else:
            first_index = 0
            end_index = len(data_asset_records)
            second_index = 1
            responses = []
            record_json = data_asset_records[first_index].json(by_alias=True)
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
                    record_json = data_asset_records[second_index].json(
                        by_alias=True
                    )
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
