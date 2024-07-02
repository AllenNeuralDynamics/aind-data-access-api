"""Tests methods in utils module"""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_access_api.utils import (
    build_docdb_location_to_id_map,
    does_metadata_record_exist_in_docdb,
    get_record_from_docdb,
    get_s3_bucket_and_prefix,
    get_s3_location,
    is_dict_corrupt,
    paginate_docdb,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
TEST_UTILS_DIR = TEST_DIR / "resources" / "utils"


class TestUtils(unittest.TestCase):
    """Class to test methods in utils module."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class by extracting contents from example files."""

        def load_json_file(filename: str) -> dict:
            """Load json file from resources directory."""
            with open(TEST_UTILS_DIR / filename, "r") as f:
                return json.load(f)

        example_metadata_nd = load_json_file("example_metadata.nd.json")
        example_metadata_nd1 = load_json_file("example_metadata1.nd.json")
        example_metadata_nd2 = load_json_file("example_metadata2.nd.json")

        cls.example_metadata_nd = example_metadata_nd
        cls.example_metadata_nd1 = example_metadata_nd1
        cls.example_metadata_nd2 = example_metadata_nd2

    def test_is_dict_corrupt(self):
        """Tests is_dict_corrupt method"""
        good_contents = {"a": 1, "b": {"c": 2, "d": 3}}
        bad_contents1 = {"a.1": 1, "b": {"c": 2, "d": 3}}
        bad_contents2 = {"a": 1, "b": {"c": 2, "$d": 3}}
        bad_contents3 = {"a": 1, "b": {"c": 2, "d": 3}, "$e": 4}
        bad_contents4 = {"a": 1, "b": {"c": {"d": 3}, "$e": 4}}
        bad_contents5 = [{"a": 1}, {"b": {"c": 2, "d": 3}}]
        self.assertFalse(is_dict_corrupt(good_contents))
        self.assertTrue(is_dict_corrupt(bad_contents1))
        self.assertTrue(is_dict_corrupt(bad_contents2))
        self.assertTrue(is_dict_corrupt(bad_contents3))
        self.assertTrue(is_dict_corrupt(bad_contents4))
        self.assertTrue(is_dict_corrupt(bad_contents5))

    def test_get_s3_bucket_and_prefix(self):
        """Tests get_s3_bucket_and_prefix"""
        results1 = get_s3_bucket_and_prefix(
            s3_location="s3://some_bucket/prefix1/"
        )
        results2 = get_s3_bucket_and_prefix(
            s3_location="s3://some_bucket/prefix2"
        )

        self.assertEqual(
            {"bucket": "some_bucket", "prefix": "prefix1"}, results1
        )
        self.assertEqual(
            {"bucket": "some_bucket", "prefix": "prefix2"}, results2
        )

    def test_get_s3_location(self):
        """Tests get_s3_location"""
        result1 = get_s3_location(bucket="some_bucket", prefix="prefix1")
        result2 = get_s3_location(bucket="some_bucket", prefix="prefix2/")
        self.assertEqual("s3://some_bucket/prefix1", result1)
        self.assertEqual("s3://some_bucket/prefix2", result2)

    @patch("pymongo.MongoClient")
    def test_does_metadata_record_exist_in_docdb_true(
        self, mock_docdb_client: MagicMock
    ):
        """Tests does_metadata_record_exist_in_docdb when true"""

        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [{"_id": "70bcf356-985f-4a2a-8105-de900e35e788"}]
        )
        self.assertTrue(
            does_metadata_record_exist_in_docdb(
                docdb_client=mock_docdb_client,
                db_name="metadata_index",
                collection_name="data_assets",
                bucket="aind-ephys-data-dev-u5u0i5",
                prefix="ecephys_642478_2023-01-17_13-56-29",
            )
        )

    @patch("pymongo.MongoClient")
    def test_does_metadata_record_exist_in_docdb_false(
        self, mock_docdb_client: MagicMock
    ):
        """Tests does_metadata_record_exist_in_docdb when false"""

        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([])
        self.assertFalse(
            does_metadata_record_exist_in_docdb(
                docdb_client=mock_docdb_client,
                db_name="metadata_index",
                collection_name="data_assets",
                bucket="aind-ephys-data-dev-u5u0i5",
                prefix="ecephys_642478_2023-01-17_13-56-29",
            )
        )

    @patch("pymongo.MongoClient")
    def test_get_record_from_docdb(self, mock_docdb_client: MagicMock):
        """Tests get_record_from_docdb when record exists"""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([self.example_metadata_nd])
        record = get_record_from_docdb(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            record_id="488bbe42-832b-4c37-8572-25eb87cc50e2",
        )
        self.assertEqual(self.example_metadata_nd, record)

    @patch("pymongo.MongoClient")
    def test_get_record_from_docdb_none(self, mock_docdb_client: MagicMock):
        """Tests get_record_from_docdb when record doesn't exist"""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([])
        record = get_record_from_docdb(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            record_id="488bbe42-832b-4c37-8572-25eb87cc50ee",
        )
        self.assertIsNone(record)

    @patch("pymongo.MongoClient")
    def test_paginate_docdb(self, mock_docdb_client: MagicMock):
        """Tests paginate_docdb"""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [
                self.example_metadata_nd,
                self.example_metadata_nd1,
                self.example_metadata_nd2,
            ]
        )
        pages = paginate_docdb(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            page_size=2,
        )
        expected_results = [
            [self.example_metadata_nd, self.example_metadata_nd1],
            [self.example_metadata_nd2],
        ]
        actual_results = list(pages)
        self.assertEqual(expected_results, actual_results)

    @patch("pymongo.MongoClient")
    def test_build_docdb_location_to_id_map(
        self, mock_docdb_client: MagicMock
    ):
        """Tests build_docdb_location_to_id_map"""
        bucket = "aind-ephys-data-dev-u5u0i5"
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [
                {
                    "_id": "70bcf356-985f-4a2a-8105-de900e35e788",
                    "location": (
                        f"s3://{bucket}/ecephys_655019_2000-04-04_04-00-00"
                    ),
                },
                {
                    "_id": "5ca4a951-d374-4f4b-8279-d570a35b2286",
                    "location": (
                        f"s3://{bucket}/ecephys_567890_2000-01-01_04-00-00"
                    ),
                },
            ]
        )

        actual_map = build_docdb_location_to_id_map(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            bucket=bucket,
            prefixes=[
                "ecephys_655019_2000-04-04_04-00-00",
                "ecephys_567890_2000-01-01_04-00-00/",
                "missing_655019_2000-01-01_01-01-02",
            ],
        )
        expected_map = {
            f"s3://{bucket}/ecephys_655019_2000-04-04_04-00-00": (
                "70bcf356-985f-4a2a-8105-de900e35e788"
            ),
            f"s3://{bucket}/ecephys_567890_2000-01-01_04-00-00": (
                "5ca4a951-d374-4f4b-8279-d570a35b2286"
            ),
        }
        self.assertEqual(expected_map, actual_map)


if __name__ == "__main__":
    unittest.main()
