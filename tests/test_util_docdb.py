"""Tests methods in util.docdb module"""


import unittest
from unittest.mock import MagicMock

from aind_data_access_api.util.docdb import (
    get_record_from_docdb,
    get_id_from_name,
    get_projected_record_from_docdb,
)


class TestUtilDocDB(unittest.TestCase):
    """Class to test methods in util.docdb module."""

    def test_get_id_from_name(self):
        """Tests get_id_from_name"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "name": "123"}
        ]
        self.assertEqual("abcd", get_id_from_name(client, name="123"))

    def test_get_record_from_docdb(self):
        """Tests get_record_from_docdb"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [{"_id": "abcd"}]
        record = get_record_from_docdb(client, record_id="abcd")
        self.assertEqual({"_id": "abcd"}, record)

    def test_get_projected_record_from_docdb(self):
        """Tests get_projected_record_from_docdb"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": {"a": 1}}
        ]
        record = get_projected_record_from_docdb(
            client, record_id="abcd", projection={"quality_control": 1}
        )
        self.assertEqual({"_id": "abcd", "quality_control": {"a": 1}}, record)