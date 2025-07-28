"""Test document_db module."""

import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import requests.exceptions
from requests import Response

from aind_data_access_api.document_db import (
    AnalysisDbClient,
    Client,
    MetadataDbClient,
    SchemaDbClient,
)


class TestClient(unittest.TestCase):
    """Test methods in Client class."""

    example_client_args = {
        "host": "example.com/",
        "database": "db",
        "collection": "coll",
    }

    example_record_list = [
        {
            "_id": f"{id_num}",
            "name": "modal_00000_2000-10-10_10-10-10",
            "location": "some_url",
            "created": datetime(2000, 10, 10, 10, 10, 10),
            "subject": {"subject_id": "00000", "sex": "Female"},
        }
        for id_num in range(0, 10)
    ]

    def test_client_constructor(self):
        """Tests class constructor"""
        client = Client(**self.example_client_args)

        self.assertEqual("example.com", client.host)
        self.assertEqual("db", client.database)
        self.assertEqual("coll", client.collection)
        self.assertEqual("https://example.com/v1/db/coll", client._base_url)
        self.assertEqual(
            "https://example.com/v1/db/coll/count_documents", client._count_url
        )
        self.assertEqual(
            "https://example.com/v1/db/coll/find", client._find_url
        )
        self.assertEqual(
            "https://example.com/v1/db/coll/update_one",
            client._update_one_url,
        )
        self.assertEqual(
            "https://example.com/v1/db/coll/bulk_write",
            client._bulk_write_url,
        )

    @patch("requests.Session.get")
    def test__count_records(self, mock_get: MagicMock):
        """Tests _count_records method"""

        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 200
        mock_records_counts = {
            "total_record_count": 1234,
            "filtered_record_count": 47,
        }
        mock_response._content = json.dumps(mock_records_counts).encode(
            "utf-8"
        )
        mock_get.return_value = mock_response
        record_count = client._count_records(filter_query={"_id": "abc"})
        self.assertEqual(
            mock_records_counts,
            record_count,
        )

    @patch("requests.Session.get")
    def test__count_records_error(self, mock_get: MagicMock):
        """Tests _count_records when there is a HTTP error"""
        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 400
        mock_get.return_value = mock_response
        with self.assertRaises(requests.exceptions.HTTPError) as e:
            client._count_records(filter_query={"_id": "abc"})
        self.assertIn("400 Client Error", str(e.exception))

    @patch("requests.Session.get")
    def test__find_records(self, mock_get: MagicMock):
        """Tests _find_records method"""

        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(
            [
                {"_id": "abc123", "message": "hi"},
                {"_id": "efg456", "message": "hello"},
            ]
        ).encode("utf-8")
        mock_response2 = Response()
        mock_response2.status_code = 200
        mock_response2._content = json.dumps(
            [{"_id": "abc123", "message": "hi"}]
        ).encode("utf-8")
        mock_response3 = Response()
        mock_response3.status_code = 200
        mock_response3._content = None

        mock_get.side_effect = [mock_response, mock_response2, mock_response3]
        records1 = client._find_records()
        records2 = client._find_records(
            filter_query={"_id": "abc123"},
            projection={"_id": 1, "message": 1},
            sort={"message": 1},
            limit=1,
        )
        self.assertEqual(
            [
                {"_id": "abc123", "message": "hi"},
                {"_id": "efg456", "message": "hello"},
            ],
            records1,
        )
        self.assertEqual([{"_id": "abc123", "message": "hi"}], records2)

    @patch("requests.Session.get")
    def test__find_records_error(self, mock_get: MagicMock):
        """Tests _find_records method when there is an HTTP error or
        no payload in response"""
        client = Client(**self.example_client_args)
        mock_response1 = Response()
        mock_response1.status_code = 404
        mock_response2 = Response()
        mock_response2.status_code = 200
        mock_response2._content = None
        mock_get.side_effect = [mock_response1, mock_response2]
        with self.assertRaises(requests.exceptions.HTTPError) as e:
            client._find_records(filter_query={"_id": "abc"})
        self.assertIn("404 Client Error", str(e.exception))
        with self.assertRaises(requests.exceptions.JSONDecodeError) as e:
            client._get_records(filter_query={"_id": "4532654"})
        self.assertIn(
            "Expecting value: line 1 column 1 (char 0)", str(e.exception)
        )

    @patch("requests.Session.get")
    def test__get_records(self, mock_get: MagicMock):
        """Tests _get_records method"""

        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(
            [
                {"_id": "abc123", "message": "hi"},
                {"_id": "efg456", "message": "hello"},
            ]
        ).encode("utf-8")
        mock_response2 = Response()
        mock_response2.status_code = 200
        mock_response2._content = json.dumps(
            [{"_id": "abc123", "message": "hi"}]
        ).encode("utf-8")
        mock_response3 = Response()
        mock_response3.status_code = 200
        mock_response3._content = None

        mock_get.side_effect = [mock_response, mock_response2, mock_response3]
        records1 = client._get_records()
        records2 = client._get_records(
            filter_query={"_id": "abc123"},
            projection={"_id": 1, "message": 1},
            sort={"message": 1},
        )
        self.assertEqual(
            [
                {"_id": "abc123", "message": "hi"},
                {"_id": "efg456", "message": "hello"},
            ],
            records1,
        )
        self.assertEqual([{"_id": "abc123", "message": "hi"}], records2)

    @patch("requests.Session.get")
    def test__get_records_error(self, mock_get: MagicMock):
        """Tests _get_records method when there is an HTTP error or
        no payload in response"""
        client = Client(**self.example_client_args)
        mock_response1 = Response()
        mock_response1.status_code = 404
        mock_response2 = Response()
        mock_response2.status_code = 200
        mock_response2._content = None
        mock_get.side_effect = [mock_response1, mock_response2]
        with self.assertRaises(requests.exceptions.HTTPError) as e:
            client._get_records(filter_query={"_id": "abc"})
        self.assertIn("404 Client Error", str(e.exception))
        with self.assertRaises(requests.exceptions.JSONDecodeError) as e:
            client._get_records(filter_query={"_id": "4532654"})
        self.assertIn(
            "Expecting value: line 1 column 1 (char 0)", str(e.exception)
        )

    @patch("requests.Session.post")
    def test__aggregate_records(self, mock_post: MagicMock):
        """Tests _aggregate_records method"""
        pipeline = [{"$match": {"_id": "abc123"}}]
        client = Client(**self.example_client_args)
        mock_response = Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(
            [{"_id": "abc123", "message": "hi"}]
        ).encode("utf-8")

        mock_post.return_value = mock_response
        result = client._aggregate_records(pipeline=pipeline)
        self.assertEqual(
            [{"_id": "abc123", "message": "hi"}],
            result,
        )

    @patch("requests.Session.post")
    def test__aggregate_records_error(self, mock_post: MagicMock):
        """Tests _aggregate_records method when there is an HTTP error or
        no payload in response"""
        invalid_pipeline = [{"$match_invalid": {"_id": "abc123"}}]
        client = Client(**self.example_client_args)
        mock_response1 = Response()
        mock_response1.status_code = 400
        mock_response2 = Response()
        mock_response2.status_code = 200
        mock_response2._content = None
        mock_post.side_effect = [mock_response1, mock_response2]
        with self.assertRaises(requests.exceptions.HTTPError) as e:
            client._aggregate_records(pipeline=invalid_pipeline)
        self.assertIn("400 Client Error", str(e.exception))
        with self.assertRaises(requests.exceptions.JSONDecodeError) as e:
            client._aggregate_records(pipeline=invalid_pipeline)
        self.assertIn(
            "Expecting value: line 1 column 1 (char 0)", str(e.exception)
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.Session.post")
    def test__insert_one_record(
        self,
        mock_post: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests _insert_one method"""
        mock_creds = MagicMock()
        mock_creds.access_key = "abc"
        mock_creds.secret_key = "efg"
        mock_session.return_value.region_name = "us-west-2"
        mock_session.get_credentials.return_value = mock_creds

        client = Client(**self.example_client_args)
        client._insert_one_record(
            {"_id": "123", "message": "hi"},
        )
        mock_auth.assert_called_once()
        mock_post.assert_called_once_with(
            url="https://example.com/v1/db/coll/insert_one",
            headers={"Content-Type": "application/json"},
            data=('{"_id": "123", "message": "hi"}'),
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.Session.post")
    def test__upsert_one_record(
        self,
        mock_post: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests _upsert_one method"""
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
            url="https://example.com/v1/db/coll/update_one",
            headers={"Content-Type": "application/json"},
            data=(
                '{"filter": {"_id": "123"},'
                ' "update": {"$set": {"_id": "123", "message": "hi"}},'
                ' "upsert": "True"}'
            ),
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.Session.post")
    def test__bulk_write(
        self,
        mock_post: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests _bulk_write method"""
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
            url="https://example.com/v1/db/coll/bulk_write",
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

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.Session.delete")
    def test__delete_one_record(
        self,
        mock_delete: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests _delete_one method"""
        mock_creds = MagicMock()
        mock_creds.access_key = "abc"
        mock_creds.secret_key = "efg"
        mock_session.return_value.region_name = "us-west-2"
        mock_session.get_credentials.return_value = mock_creds

        client = Client(**self.example_client_args)
        client._delete_one_record(record_filter={"_id": "123"})
        mock_auth.assert_called_once()
        mock_delete.assert_called_once_with(
            url="https://example.com/v1/db/coll/delete_one",
            headers={"Content-Type": "application/json"},
            data=('{"filter": {"_id": "123"}}'),
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.Session.delete")
    def test__delete_many_records(
        self,
        mock_delete: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests _delete_many_records method"""
        mock_creds = MagicMock()
        mock_creds.access_key = "abc"
        mock_creds.secret_key = "efg"
        mock_session.return_value.region_name = "us-west-2"
        mock_session.get_credentials.return_value = mock_creds

        client = Client(**self.example_client_args)
        client._delete_many_records(
            record_filter={"_id": {"$in": ["123", "456"]}}
        )
        mock_auth.assert_called_once()
        mock_delete.assert_called_once_with(
            url="https://example.com/v1/db/coll/delete_many",
            headers={"Content-Type": "application/json"},
            data=('{"filter": {"_id": {"$in": ["123", "456"]}}}'),
        )

    @patch("aind_data_access_api.document_db.Client._find_records")
    def test_retrieve_docdb_records(
        self,
        mock_find_records_response: MagicMock,
    ):
        """Tests retrieving docdb records"""
        client = Client(**self.example_client_args)
        mock_find_records_response.side_effect = [
            self.example_record_list[0:2],
            self.example_record_list[2:6],
            self.example_record_list[6:7],
            self.example_record_list[7:10],
            [],
        ]
        with self.assertLogs(level="DEBUG") as captured:
            records = client.retrieve_docdb_records()
        expected_logs = [
            "DEBUG:root:(skip=0, limit=0): Retrieved 2 records",
            "DEBUG:root:(skip=2, limit=0): Retrieved 4 records",
            "DEBUG:root:(skip=6, limit=0): Retrieved 1 records",
            "DEBUG:root:(skip=7, limit=0): Retrieved 3 records",
            "DEBUG:root:(skip=10, limit=0): Retrieved 0 records",
        ]
        self.assertEqual(expected_logs, captured.output)
        self.assertEqual(self.example_record_list, records)

    @patch("aind_data_access_api.document_db.Client._find_records")
    def test_retrieve_many_docdb_records_limit(
        self,
        mock_find_records_response: MagicMock,
    ):
        """Tests retrieving docdb records when the limit is reached"""
        client = Client(**self.example_client_args)
        mock_find_records_response.side_effect = [
            self.example_record_list[0:2],
            self.example_record_list[2:5],
        ]
        with self.assertLogs(level="DEBUG") as captured:
            records = client.retrieve_docdb_records(limit=5)
        expected_log_messages = [
            "DEBUG:root:(skip=0, limit=5): Retrieved 2 records",
            "DEBUG:root:(skip=2, limit=3): Retrieved 3 records",
        ]
        self.assertEqual(expected_log_messages, captured.output)
        self.assertEqual(self.example_record_list[0:5], records)

    @patch("aind_data_access_api.document_db.Client._find_records")
    def test_retrieve_many_docdb_records_limit_not_reached(
        self,
        mock_find_records_response: MagicMock,
    ):
        """Tests retrieving docdb records when the limit is not reached"""
        client = Client(**self.example_client_args)
        mock_find_records_response.side_effect = [
            self.example_record_list[0:2],
            self.example_record_list[2:6],
            self.example_record_list[6:7],
            self.example_record_list[7:10],
            [],
        ]
        with self.assertLogs(level="DEBUG") as captured:
            records = client.retrieve_docdb_records(limit=20)
        expected_log_messages = [
            "DEBUG:root:(skip=0, limit=20): Retrieved 2 records",
            "DEBUG:root:(skip=2, limit=18): Retrieved 4 records",
            "DEBUG:root:(skip=6, limit=14): Retrieved 1 records",
            "DEBUG:root:(skip=7, limit=13): Retrieved 3 records",
            "DEBUG:root:(skip=10, limit=10): Retrieved 0 records",
        ]
        self.assertEqual(expected_log_messages, captured.output)
        self.assertEqual(self.example_record_list, records)

    @patch("aind_data_access_api.document_db.Client._find_records")
    def test_retrieve_docdb_records_none(
        self,
        mock_find_records_response: MagicMock,
    ):
        """Tests retrieving docdb records"""
        client = Client(**self.example_client_args)
        mock_find_records_response.return_value = []
        with self.assertLogs(level="DEBUG") as captured:
            records = client.retrieve_docdb_records()
        self.assertEqual(
            ["DEBUG:root:(skip=0, limit=0): Retrieved 0 records"],
            captured.output,
        )
        self.assertEqual([], records)

    @patch("aind_data_access_api.document_db.Client._aggregate_records")
    def test_aggregate_docdb_records(self, mock_aggregate: MagicMock):
        """Tests aggregating docdb records"""
        expected_result = [
            {
                "_id": "abc-123",
                "name": "modal_00000_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Female"},
            }
        ]
        client = Client(**self.example_client_args)
        mock_aggregate.return_value = expected_result
        pipeline = [{"$match": {"_id": "abc-123"}}]
        result = client.aggregate_docdb_records(pipeline)
        self.assertEqual(result, expected_result)
        mock_aggregate.assert_called_once_with(
            pipeline=pipeline,
        )

    @patch("aind_data_access_api.document_db.Client._insert_one_record")
    def test_insert_one_docdb_record(self, mock_insert: MagicMock):
        """Tests inserting one docdb record"""
        client = Client(**self.example_client_args)
        mock_insert.return_value = {"message": "success"}
        record = {
            "_id": "abc-123",
            "name": "modal_00000_2000-10-10_10-10-10",
            "created": datetime(2000, 10, 10, 10, 10, 10),
            "location": "some_url",
            "subject": {"subject_id": "00000", "sex": "Female"},
        }
        response = client.insert_one_docdb_record(record)
        self.assertEqual({"message": "success"}, response)
        mock_insert.assert_called_once_with(
            json.loads(json.dumps(record, default=str)),
        )

    @patch("aind_data_access_api.document_db.Client._upsert_one_record")
    def test_upsert_one_docdb_record(self, mock_upsert: MagicMock):
        """Tests upserting one docdb record"""
        client = Client(**self.example_client_args)
        mock_upsert.return_value = {"message": "success"}
        record = {
            "_id": "abc-123",
            "name": "modal_00000_2000-10-10_10-10-10",
            "created": datetime(2000, 10, 10, 10, 10, 10),
            "location": "some_url",
            "subject": {"subject_id": "00000", "sex": "Female"},
        }
        response = client.upsert_one_docdb_record(record)
        self.assertEqual({"message": "success"}, response)
        mock_upsert.assert_called_once_with(
            record_filter={"_id": "abc-123"},
            update={"$set": json.loads(json.dumps(record, default=str))},
        )

    @patch("aind_data_access_api.document_db.Client._upsert_one_record")
    def test_upsert_one_docdb_record_invalid(self, mock_upsert: MagicMock):
        """Tests upserting one docdb record if record is invalid"""
        client = Client(**self.example_client_args)
        record_no__id = {
            "id": "abc-123",
            "name": "modal_00000_2000-10-10_10-10-10",
            "created": datetime(2000, 10, 10, 10, 10, 10),
            "location": "some_url",
            "subject": {"subject_id": "00000", "sex": "Female"},
        }
        with self.assertRaises(ValueError) as e:
            client.upsert_one_docdb_record(record_no__id)
        self.assertEqual(
            "Record does not have an _id field.", str(e.exception)
        )
        mock_upsert.assert_not_called()

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_list_of_docdb_records(self, mock_bulk_write: MagicMock):
        """Tests upserting a list of docdb records"""

        client = Client(**self.example_client_args)
        mock_bulk_write.return_value = {"message": "success"}
        records = [
            {
                "_id": "abc-123",
                "name": "modal_00000_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Female"},
            },
            {
                "_id": "abc-125",
                "name": "modal_00001_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Male"},
            },
        ]
        response = client.upsert_list_of_docdb_records(records)
        self.assertEqual([{"message": "success"}], response)
        mock_bulk_write.assert_called_once_with(
            [
                {
                    "UpdateOne": {
                        "filter": {"_id": "abc-123"},
                        "update": {
                            "$set": json.loads(
                                json.dumps(records[0], default=str)
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
                                json.dumps(records[1], default=str)
                            )
                        },
                        "upsert": "True",
                    }
                },
            ]
        )

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_empty_list_of_docdb_records(
        self, mock_bulk_write: MagicMock
    ):
        """Tests upserting an empty list of docdb records"""

        client = Client(**self.example_client_args)
        records = []

        response = client.upsert_list_of_docdb_records(records)
        self.assertEqual([], response)
        mock_bulk_write.assert_not_called()

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_chunked_list_of_docdb_records(
        self, mock_bulk_write: MagicMock
    ):
        """Tests upserting a list of docdb records in chunks"""

        client = Client(**self.example_client_args)
        mock_bulk_write.return_value = {"message": "success"}
        records = [
            {
                "_id": "abc-123",
                "name": "modal_00000_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Female"},
            },
            {
                "_id": "abc-125",
                "name": "modal_00001_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Male"},
            },
        ]

        response = client.upsert_list_of_docdb_records(
            records, max_payload_size=1
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
                                    "$set": json.loads(
                                        json.dumps(records[0], default=str)
                                    )
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
                                    "$set": json.loads(
                                        json.dumps(records[1], default=str)
                                    )
                                },
                                "upsert": "True",
                            }
                        }
                    ]
                ),
            ]
        )

    @patch("aind_data_access_api.document_db.Client._bulk_write")
    def test_upsert_list_of_docdb_records_invalid(
        self, mock_bulk_write: MagicMock
    ):
        """Tests upserting a list of docdb records if a record is invalid"""

        client = Client(**self.example_client_args)
        records_no__id = [
            {
                "_id": "abc-123",
                "name": "modal_00000_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Female"},
            },
            {
                "id": "abc-125",
                "name": "modal_00001_2000-10-10_10-10-10",
                "created": datetime(2000, 10, 10, 10, 10, 10),
                "location": "some_url",
                "subject": {"subject_id": "00000", "sex": "Male"},
            },
        ]
        with self.assertRaises(ValueError) as e:
            client.upsert_list_of_docdb_records(records_no__id)
        self.assertEqual(
            "A record does not have an _id field.", str(e.exception)
        )
        mock_bulk_write.assert_not_called()

    @patch("aind_data_access_api.document_db.Client._delete_one_record")
    def test_delete_one_record(self, mock_delete: MagicMock):
        """Tests deleting one data asset record"""
        client = Client(**self.example_client_args)
        successful_response = Response()
        successful_response.status_code = 200
        # n is the number of records removed. It will be 0 if the id does
        # exist
        response_message = {
            "n": 1,
            "ok": 1.0,
            "operationTime": {"$timestamp": {"t": 1707262037, "i": 1}},
        }
        successful_response._content = json.dumps(response_message).encode(
            "utf-8"
        )
        mock_delete.return_value = successful_response
        response = client.delete_one_record("abc-123")
        self.assertEqual(successful_response.json(), response.json())
        mock_delete.assert_called_once_with(
            record_filter={"_id": "abc-123"},
        )

    @patch("aind_data_access_api.document_db.Client._delete_many_records")
    def test_delete_many_records(self, mock_delete: MagicMock):
        """Tests deleting many data asset records"""
        client = Client(**self.example_client_args)
        successful_response = Response()
        successful_response.status_code = 200
        # n is the number of records removed. It will be 0 if the id does
        # exist
        response_message = {
            "n": 2,
            "ok": 1.0,
            "operationTime": {"$timestamp": {"t": 1707262037, "i": 1}},
        }
        successful_response._content = json.dumps(response_message).encode(
            "utf-8"
        )
        mock_delete.return_value = successful_response
        response = client.delete_many_records(["abc-123", "def-456"])
        self.assertEqual(successful_response.json(), response.json())
        mock_delete.assert_called_once_with(
            record_filter={"_id": {"$in": ["abc-123", "def-456"]}},
        )

    @patch("aind_data_access_api.document_db.Session")
    def test_content_manager(
        self,
        mock_session: MagicMock,
    ):
        """Tests request session closes when client is in context manager."""
        mock_response = Response()
        mock_response.status_code = 200
        mock_records_counts = {
            "total_record_count": 1234,
            "filtered_record_count": 47,
        }
        mock_response._content = json.dumps(mock_records_counts).encode(
            "utf-8"
        )
        mock_session.return_value.get.return_value = mock_response
        with Client(**self.example_client_args) as client:
            record_count = client._count_records(filter_query={"_id": "abc"})
        self.assertEqual(
            mock_records_counts,
            record_count,
        )
        mock_session.assert_has_calls(
            [
                call(),
                call().get(
                    "https://example.com/v1/db/coll/count_documents",
                    params={
                        "filter": '{"_id": "abc"}',
                    },
                ),
                call().close(),
            ]
        )


class TestMetadataDbClient(unittest.TestCase):
    """Test methods in MetadataDbClient class."""

    example_client_args = {
        "host": "example.com/",
    }

    def test_metadatadbclient_constructor(self):
        """Tests class constructor"""
        client = MetadataDbClient(**self.example_client_args)

        self.assertEqual("example.com", client.host)
        self.assertEqual("metadata_index", client.database)
        self.assertEqual("data_assets", client.collection)
        self.assertEqual("v1", client.version)
        self.assertEqual(
            "https://example.com/v1/metadata_index/data_assets",
            client._base_url,
        )
        self.assertEqual(
            "https://example.com/v1/data_summary", client._data_summary_url
        )

        client = MetadataDbClient(**self.example_client_args, version="v2")
        self.assertEqual("v2", client.version)
        self.assertEqual(
            "https://example.com/v2/metadata_index/data_assets",
            client._base_url,
        )
        self.assertEqual(
            "https://example.com/v2/data_summary", client._data_summary_url
        )

    @patch("boto3.session.Session")
    @patch("botocore.auth.SigV4Auth.add_auth")
    @patch("requests.Session.get")
    def test_generate_data_summary(
        self,
        mock_get: MagicMock,
        mock_auth: MagicMock,
        mock_session: MagicMock,
    ):
        """Tests generate_data_summary method"""
        mock_creds = MagicMock()
        mock_creds.access_key = "abc"
        mock_creds.secret_key = "efg"
        mock_session.return_value.region_name = "us-west-2"
        mock_session.get_credentials.return_value = mock_creds
        mock_response = Response()
        mock_response.status_code = 200
        response_message = {
            "message": "Summary of data asset.",
            "parameters": {
                "docdb_id": "abc-123",
                "langchain_hub_prompt_id": "prompt_id",
                "bedrock_chat_model_name": "model_name",
                "bedrock_chat_model_kwargs": {"temperature": 0},
            },
        }
        mock_response._content = json.dumps(response_message).encode("utf-8")
        mock_get.return_value = mock_response

        client = MetadataDbClient(**self.example_client_args)
        response = client.generate_data_summary("abc-123")
        mock_auth.assert_called_once()
        mock_get.assert_called_once_with(
            url="https://example.com/v1/data_summary/abc-123",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response_message, response)


class TestAnalysisDbClient(unittest.TestCase):
    """Test methods in AnalysisDbClient class."""

    example_client_args = {
        "host": "example.com/",
        "collection": "dynamic-foraging-nm",
    }

    def test_analysisdbclient_constructor(self):
        """Tests class constructor"""
        client = AnalysisDbClient(**self.example_client_args)

        self.assertEqual("example.com", client.host)
        self.assertEqual("analysis", client.database)
        self.assertEqual("dynamic-foraging-nm", client.collection)
        self.assertEqual("v1", client.version)
        self.assertEqual(
            "https://example.com/v1/analysis/dynamic-foraging-nm",
            client._base_url,
        )


class TestSchemaDbClient(unittest.TestCase):
    """Test methods in SchemaDbClient"""

    @patch("aind_data_access_api.document_db.Client._get_records")
    def test_retrieve_schema_records(
        self,
        mock_get_record_response: MagicMock,
    ):
        """Tests retrieving schema records"""

        schema_type = "procedures"
        schema_version = "abc-123"
        client = SchemaDbClient(host="example.com/", collection=schema_type)
        expected_response = [
            {
                "_id": "abc-123",
                "description": "Mock procedure schema",
                "title": "Mock Procedures",
                "definitions": {"MassUnit": object, "TimeUnit": object},
                "properties": {"schema_version": object, "subject_id": object},
            }
        ]
        mock_get_record_response.return_value = expected_response
        records = client.retrieve_schema_records(schema_version=schema_version)
        mock_get_record_response.assert_called_once_with(
            filter_query={"_id": "abc-123"},
            projection=None,
            sort=None,
            limit=0,
        )
        self.assertEqual(expected_response, records)


if __name__ == "__main__":
    unittest.main()
