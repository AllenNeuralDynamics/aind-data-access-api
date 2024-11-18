"""Tests methods in util.docdb module"""

import unittest
from unittest.mock import MagicMock

from aind_data_access_api.helpers.docdb import (
    get_record_by_id,
    get_id_from_name,
    get_projection_by_id,
    get_field_by_id,
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
        record = get_record_by_id(client, _id="abcd")
        self.assertEqual({"_id": "abcd"}, record)

        # test the empty case
        client.retrieve_docdb_records.return_value = []
        record = get_record_by_id(client, _id="abcd")
        self.assertIsNone(record)

    def test_get_projected_record_from_docdb(self):
        """Tests get_projected_record_from_docdb"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"quality_control": {"a": 1}}
        ]
        record = get_projection_by_id(
            client, _id="abcd", projection={"quality_control": 1}
        )
        self.assertEqual({"quality_control": {"a": 1}}, record)

    def test_get_field_from_docdb(self):
        """Tests get_field_from_docdb"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"quality_control": {"a": 1}}
        ]
        field = get_field_by_id(client, _id="abcd", field="quality_control")
        self.assertEqual({"quality_control": {"a": 1}}, field)
