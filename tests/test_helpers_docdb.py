"""Tests methods in helpers.docdb module"""

import unittest
from unittest.mock import MagicMock, patch

from aind_data_access_api.helpers.docdb import (
    get_field_by_id,
    get_id_from_name,
    get_name_from_id,
    get_projection_by_id,
    get_record_by_id,
)


class TestHelpersDocDB(unittest.TestCase):
    """Class to test methods in helpers.docdb module."""

    def test_get_id_from_name(self):
        """Tests get_id_from_name"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "name": "123"}
        ]
        self.assertEqual("abcd", get_id_from_name(client, name="123"))

    @patch("logging.warning")
    def test_get_id_from_name_multiple(self, mock_warning):
        """Tests get_id_from_name with multiple records"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd"},
            {"_id": "efgh"},
        ]
        result = get_id_from_name(client, name="123")
        self.assertEqual("abcd", result)
        mock_warning.assert_called_once_with(
            "Multiple records share the name 123, "
            "only the first record will be returned."
        )

    def test_get_id_from_name_error(self):
        """Tests get_id_from_name with no records"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = []
        with self.assertRaises(ValueError) as e:
            get_id_from_name(client, name="123")
        self.assertEqual("No record found with name 123", str(e.exception))

    def test_get_name_from_id(self):
        """Tests get_name_from_id"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "name": "123"}
        ]
        result = get_name_from_id(client, _id="abcd")
        self.assertEqual("123", result)

    def test_get_name_from_id_error(self):
        """Tests get_name_from_id with no records"""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = []
        with self.assertRaises(ValueError) as e:
            get_name_from_id(client, _id="abcd")
        self.assertEqual("No record found with _id abcd", str(e.exception))

    def test_get_record_by_id(self):
        """Tests get_record_by_id"""
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


if __name__ == "__main__":
    unittest.main()
