"""Test document_db module."""

import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, call, patch

from requests import Response

from aind_data_access_api.document_db import Client, MetadataDbClient
from aind_data_access_api.models import DataAssetRecord


class TestClient(unittest.TestCase):
    """Test methods in Client class."""

    example_client_args = {
        "host": "acmecorp.com/",
        "database": "db",
        "collection": "coll",
    }

    def test_client_constructor(self):
        """Tests class constructor"""
        client = Client(**self.example_client_args)

        self.assertEqual("acmecorp.com", client.host)
        self.assertEqual("db", client.database)
        self.assertEqual("coll", client.collection)
        self.assertEqual("https://acmecorp.com/v1/db/coll", client._base_url)
        self.assertEqual(
            "https://acmecorp.com/v1/db/coll/update_one",
            client._update_one_url,
        )
        self.assertEqual(
            "https://acmecorp.com/v1/db/coll/bulk_write",
            client._bulk_write_url,
        )

    @patch("requests.get")
    def test_count_records(self, mock_get: MagicMock):
        """Tests get_records method"""

        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 200
        mock_records_counts = {
            "total_record_count": 1234,
            "filtered_record_count": 47,
        }
        body = json.dumps(mock_records_counts)
        mock_response._content = json.dumps({"body": body}).encode("utf-8")
        mock_get.return_value = mock_response
        record_count = client._count_records(filter_query={"_id": "abc"})
        self.assertEqual(
            mock_records_counts,
            record_count,
        )

    @patch("requests.get")
    def test_get_records(self, mock_get: MagicMock):
        """Tests get_records method"""

        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 200
        body = json.dumps(
            [
                {"_id": "abc123", "message": "hi"},
                {"_id": "efg456", "message": "hello"},
            ]
        )
        mock_response._content = json.dumps({"body": body}).encode("utf-8")
        mock_response2 = Response()
        mock_response2.status_code = 200
        body2 = json.dumps([{"_id": "abc123", "message": "hi"}])
        mock_response2._content = json.dumps({"body": body2}).encode("utf-8")
        mock_response3 = Response()
        mock_response3.status_code = 200
        body3 = json.dumps([{"_id": "abc123", "message": "hi"}])
        mock_response3._content = json.dumps({"miss": body3}).encode("utf-8")

        mock_get.side_effect = [mock_response, mock_response2, mock_response3]
        records1 = client._get_records()
        records2 = client._get_records(
            filter_query={"_id": "abc123"},
            projection={"_id": 1, "message": 1},
            sort=[("message", 1)],
        )
        with self.assertRaises(KeyError) as e:
            client._get_records(filter_query={"_id": "4532654"})
        self.assertEqual(
            [
                {"_id": "abc123", "message": "hi"},
                {"_id": "efg456", "message": "hello"},
            ],
            records1,
        )
        self.assertEqual([{"_id": "abc123", "message": "hi"}], records2)
        self.assertEqual(
            "KeyError('Body not found in json response')", repr(e.exception)
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.post")
    def test_upsert_one_record(
        self,
        mock_post: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests upsert_one method"""
        mock_creds = MagicMock()
        mock_creds.access_key = "abc"
        mock_creds.secret_key = "efg"
        mock_session.return_value.region_name = "us-west-2"
        mock_session.get_credentials.return_value = mock_creds

        client = Client(**self.example_client_args)
        client._upsert_one_record(
            record_filter={"_id": "123"},
            update={"$set": {"_id": "123", "message": "hi"}},
        )
        mock_auth.assert_called_once()
        mock_post.assert_called_once_with(
            url="https://acmecorp.com/v1/db/coll/update_one",
            headers={"Content-Type": "application/json"},
            data=(
                '{"filter": {"_id": "123"},'
                ' "update": {"$set": {"_id": "123", "message": "hi"}},'
                ' "upsert": "True"}'
            ),
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.post")
    def test_bulk_write(
        self,
        mock_post: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests bulk_write method"""
        mock_creds = MagicMock()
        mock_creds.access_key = "abc"
        mock_creds.secret_key = "efg"
        mock_session.return_value.region_name = "us-west-2"
        mock_session.get_credentials.return_value = mock_creds

        client = Client(**self.example_client_args)
        operations = [
            {
                "UpdateOne": {
                    "filter": {"_id": "abc123"},
                    "update": {"$set": {"notes": "hi"}},
                    "upsert": "True",
                }
            },
            {
                "UpdateOne": {
                    "filter": {"_id": "abc124"},
                    "update": {"$set": {"notes": "hi again"}},
                    "upsert": "True",
                }
            },
        ]
        client._bulk_write(operations=operations)
        mock_auth.assert_called_once()
        mock_post.assert_called_once_with(
            url="https://acmecorp.com/v1/db/coll/bulk_write",
            headers={"Content-Type": "application/json"},
            data=(
                '[{"UpdateOne":'
                ' {"filter": {"_id": "abc123"},'
                ' "update": {"$set": {"notes": "hi"}},'
                ' "upsert": "True"}}, '
                '{"UpdateOne":'
                ' {"filter": {"_id": "abc124"},'
                ' "update": {"$set": {"notes": "hi again"}},'
                ' "upsert": "True"}}]'
            ),
        )


class TestMetadataDbClient(unittest.TestCase):
    """Test methods in MetadataDbClient class."""

    example_client_args = {
        "host": "acmecorp.com/",
        "database": "metadata_db",
        "collection": "data_assets",
    }

    @patch("aind_data_access_api.document_db.Client._get_records")
    @patch("aind_data_access_api.document_db.Client._count_records")
    def test_retrieve_data_asset_records(
        self,
        mock_count_record_response: MagicMock,
        mock_get_record_response: MagicMock,
    ):
        """Tests retrieving data asset records"""

        client = MetadataDbClient(**self.example_client_args)
        mock_get_record_response.return_value = [
            {
                "_id": "abc-123",
                "_name": "modal_00000_2000-10-10_10-10-10",
                "_location": "some_url",
                "_created": datetime(2000, 10, 10, 10, 10, 10),
                "subject": {"subject_id": "00000", "sex": "Female"},
            }
        ]
        mock_count_record_response.return_value = {
            "total_record_count": 1,
            "filtered_record_count": 1,
        }
        expected_response = [
            DataAssetRecord(
                _id="abc-123",
                _name="modal_00000_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Female"},
            )
        ]
        records = client.retrieve_data_asset_records()
        paginate_records = client.retrieve_data_asset_records(paginate=False)
        self.assertEqual(expected_response, list(records))
        self.assertEqual(expected_response, list(paginate_records))

    @patch("aind_data_access_api.document_db.Client._get_records")
    @patch("aind_data_access_api.document_db.Client._count_records")
    @patch("logging.error")
    def test_retrieve_many_data_asset_records(
        self,
        mock_log_error: MagicMock,
        mock_count_record_response: MagicMock,
        mock_get_record_response: MagicMock,
    ):
        """Tests retrieving many data asset records"""

        client = MetadataDbClient(**self.example_client_args)
        mocked_record_list = [
            {
                "_id": f"{id_num}",
                "_name": "modal_00000_2000-10-10_10-10-10",
                "_location": "some_url",
                "_created": datetime(2000, 10, 10, 10, 10, 10),
                "subject": {"subject_id": "00000", "sex": "Female"},
            }
            for id_num in range(0, 10)
        ]
        mock_get_record_response.side_effect = [
            mocked_record_list[0:2],
            Exception("Test"),
            mocked_record_list[4:6],
            mocked_record_list[6:8],
            mocked_record_list[8:10],
        ]
        mock_count_record_response.return_value = {
            "total_record_count": len(mocked_record_list),
            "filtered_record_count": len(mocked_record_list),
        }
        expected_response = [
            DataAssetRecord(
                _id=f"{id_num}",
                _name="modal_00000_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Female"},
            )
            for id_num in [0, 1, 4, 5, 6, 7, 8, 9]
        ]
        records = client.retrieve_data_asset_records(paginate_batch_size=2)
        mock_log_error.assert_called_once_with(
            "There were errors retrieving records. [\"Exception('Test')\"]"
        )
        self.assertEqual(expected_response, list(records))

    @patch("aind_data_access_api.document_db.Client._upsert_one_record")
    def test_upsert_one_record(self, mock_upsert: MagicMock):
        """Tests upserting one data asset record"""
        client = MetadataDbClient(**self.example_client_args)
        mock_upsert.return_value = {"message": "success"}
        data_asset_record = DataAssetRecord(
            _id="abc-123",
            _name="modal_00000_2000-10-10_10-10-10",
            _created=datetime(2000, 10, 10, 10, 10, 10),
            _location="some_url",
            subject={"subject_id": "00000", "sex": "Female"},
        )
        response = client.upsert_one_record(data_asset_record)
        self.assertEqual({"message": "success"}, response)
        mock_upsert.assert_called_once_with(
            record_filter={"_id": "abc-123"},
            update={"$set": json.loads(data_asset_record.json(by_alias=True))},
        )

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_list_of_records(self, mock_bulk_write: MagicMock):
        """Tests upserting a list of data asset records"""

        client = MetadataDbClient(**self.example_client_args)
        mock_bulk_write.return_value = {"message": "success"}
        data_asset_records = [
            DataAssetRecord(
                _id="abc-123",
                _name="modal_00000_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Female"},
            ),
            DataAssetRecord(
                _id="abc-125",
                _name="modal_00001_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Male"},
            ),
        ]

        response = client.upsert_list_of_records(data_asset_records)
        self.assertEqual([{"message": "success"}], response)
        mock_bulk_write.assert_called_once_with(
            [
                {
                    "UpdateOne": {
                        "filter": {"_id": "abc-123"},
                        "update": {
                            "$set": json.loads(
                                '{"_id": "abc-123",'
                                ' "_name": "modal_00000_2000-10-10_10-10-10",'
                                ' "_created": "2000-10-10T10:10:10",'
                                ' "_location": "some_url",'
                                ' "subject":'
                                ' {"subject_id": "00000", "sex": "Female"}}'
                            )
                        },
                        "upsert": "True",
                    }
                },
                {
                    "UpdateOne": {
                        "filter": {"_id": "abc-125"},
                        "update": {
                            "$set": json.loads(
                                '{"_id": "abc-125",'
                                ' "_name": "modal_00001_2000-10-10_10-10-10",'
                                ' "_created": "2000-10-10T10:10:10",'
                                ' "_location": "some_url",'
                                ' "subject":'
                                ' {"subject_id": "00000", "sex": "Male"}}'
                            )
                        },
                        "upsert": "True",
                    }
                },
            ]
        )

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_empty_list_of_records(self, mock_bulk_write: MagicMock):
        """Tests upserting an empty list of data asset records"""

        client = MetadataDbClient(**self.example_client_args)
        data_asset_records = []

        response = client.upsert_list_of_records(data_asset_records)
        self.assertEqual([], response)
        mock_bulk_write.assert_not_called()

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_chunked_list_of_records(self, mock_bulk_write: MagicMock):
        """Tests upserting a list of data asset records in chunks"""

        client = MetadataDbClient(**self.example_client_args)
        mock_bulk_write.return_value = {"message": "success"}
        data_asset_records = [
            DataAssetRecord(
                _id="abc-123",
                _name="modal_00000_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Female"},
            ),
            DataAssetRecord(
                _id="abc-125",
                _name="modal_00001_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Male"},
            ),
        ]

        response = client.upsert_list_of_records(
            data_asset_records, max_payload_size=1
        )
        self.assertEqual(
            [{"message": "success"}, {"message": "success"}], response
        )
        mock_bulk_write.assert_has_calls(
            [
                call(
                    [
                        {
                            "UpdateOne": {
                                "filter": {"_id": "abc-123"},
                                "update": {
                                    "$set": {
                                        "_id": "abc-123",
                                        "_name": (
                                            "modal_00000_2000-10-10_10-10-10"
                                        ),
                                        "_created": "2000-10-10T10:10:10",
                                        "_location": "some_url",
                                        "subject": {
                                            "subject_id": "00000",
                                            "sex": "Female",
                                        },
                                    }
                                },
                                "upsert": "True",
                            }
                        }
                    ]
                ),
                call(
                    [
                        {
                            "UpdateOne": {
                                "filter": {"_id": "abc-125"},
                                "update": {
                                    "$set": {
                                        "_id": "abc-125",
                                        "_name": (
                                            "modal_00001_2000-10-10_10-10-10"
                                        ),
                                        "_created": "2000-10-10T10:10:10",
                                        "_location": "some_url",
                                        "subject": {
                                            "subject_id": "00000",
                                            "sex": "Male",
                                        },
                                    }
                                },
                                "upsert": "True",
                            }
                        }
                    ]
                ),
            ]
        )


if __name__ == "__main__":
    unittest.main()
