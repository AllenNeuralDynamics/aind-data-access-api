"""Module to interface with the DocumentDB"""

import json
from sys import getsizeof
from typing import List, Optional

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

    def _get_records(self, query: Optional[dict] = None) -> List[dict]:
        """Retrieve records from collection."""
        if query is None:
            response = requests.get(self._base_url)
        else:
            response = requests.get(self._base_url, params={"filter": query})
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
        self, query: Optional[dict] = None
    ) -> List[DataAssetRecord]:
        """Retrieve data asset records"""

        docdb_records = self._get_records(query=query)
        data_asset_records = []
        for record in docdb_records:
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
        return {
            "UpdateOne": {
                "filter": {"_id": record_id},
                "update": {"$set": json.loads(record)},
                "upsert": "True",
            }
        }

    def upsert_list_of_records(
        self, data_asset_records: List[DataAssetRecord], max_payload_size=10e6
    ) -> List[Response]:
        """Upsert a list of records. There's a limit to the size of the
        request that can be sent, so we chunk the requests into 5MB."""
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
