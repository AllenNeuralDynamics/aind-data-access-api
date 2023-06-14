"""Test document_store module."""

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pymongo import UpdateOne

from aind_data_access_api.document_store import (
    Client,
    DocumentStoreCredentials,
)
from aind_data_access_api.models import DataAssetRecord


class TestClient(unittest.TestCase):
    """Test methods in Client class."""

    @patch("pymongo.collection.Collection.find")
    def test_retrieve_data_asset_records(self, mock_find: MagicMock):
        """Tests that data asset records are retrieved correctly"""

        mock_find.return_value = [
            {
                "_id": "abc-123",
                "_name": "modal_00000_2000-10-10_10-10-10",
                "_location": "some_url",
                "_created": datetime(2000, 10, 10, 10, 10, 10),
                "subject": {"subject_id": "00000", "sex": "Female"},
            }
        ]

        ds_client = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="host",
                database="db",
            ),
            collection_name="coll",
        )

        records = list(ds_client.retrieve_data_asset_records(query=None))
        ds_client.close()
        expected_response = [
            DataAssetRecord(
                _id="abc-123",
                _name="modal_00000_2000-10-10_10-10-10",
                _created=datetime(2000, 10, 10, 10, 10, 10),
                _location="some_url",
                subject={"subject_id": "00000", "sex": "Female"},
            )
        ]
        self.assertEqual(expected_response, records)

    @patch("pymongo.collection.Collection.update_one")
    @patch("logging.info")
    def test_upsert_one_record(
        self, mock_log_info: MagicMock, mock_update_one: MagicMock
    ):
        """Tests upserting a single record."""
        ds_client = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
            collection_name="coll",
        )

        data_asset_record = DataAssetRecord(
            _id="abc-123",
            _name="modal_00000_2000-10-10_10-10-10",
            _created=datetime(2000, 10, 10, 10, 10, 10),
            _location="some_url",
            subject={"subject_id": "00000", "sex": "Female"},
        )

        mock_update_one.return_value = "Document Upserted"

        ds_client.upsert_one_record(data_asset_record)
        ds_client.close()

        mock_update_one.assert_called_once_with(
            {"_id": data_asset_record._id},
            {"$set": data_asset_record.dict(by_alias=True)},
            upsert=True,
        )

        mock_log_info.assert_called_once_with("Document Upserted")

    @patch("pymongo.collection.Collection.bulk_write")
    @patch("logging.info")
    def test_upsert_list_of_records(
        self, mock_log_info: MagicMock, mock_bulk_write: MagicMock
    ) -> None:
        """Tests upserting a list of records."""
        ds_client = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
            collection_name="coll",
        )

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

        mock_bulk_write.return_value = "Documents Upserted"

        ds_client.upsert_list_of_records(data_asset_records)
        ds_client.close()

        operations = [
            UpdateOne(
                {"_id": "abc-123"},
                {
                    "$set": {
                        "_id": "abc-123",
                        "_name": "modal_00000_2000-10-10_10-10-10",
                        "_created": datetime(2000, 10, 10, 10, 10, 10),
                        "_location": "some_url",
                        "subject": {"subject_id": "00000", "sex": "Female"},
                    }
                },
                True,
                None,
                None,
                None,
            ),
            UpdateOne(
                {"_id": "abc-125"},
                {
                    "$set": {
                        "_id": "abc-125",
                        "_name": "modal_00001_2000-10-10_10-10-10",
                        "_created": datetime(2000, 10, 10, 10, 10, 10),
                        "_location": "some_url",
                        "subject": {"subject_id": "00000", "sex": "Male"},
                    }
                },
                True,
                None,
                None,
                None,
            ),
        ]

        mock_bulk_write.assert_called_once_with(operations)

        mock_log_info.assert_called_once_with("Documents Upserted")

    def test_retry_writes(self):
        """Tests that the retryWrites option can be set."""
        ds_client1 = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
            collection_name="coll",
        )
        ds_client2 = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
            collection_name="coll",
            retry_writes=False,
        )

        self.assertTrue(
            ds_client1._client._MongoClient__options._options["retryWrites"]
        )
        self.assertFalse(
            ds_client2._client._MongoClient__options._options["retryWrites"]
        )


if __name__ == "__main__":
    unittest.main()
