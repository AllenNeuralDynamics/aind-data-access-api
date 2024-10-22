"""Test helpers module"""

import unittest
import json
from unittest.mock import MagicMock
from aind_data_access_api.helpers import get_quality_control
from aind_data_schema.core.quality_control import QualityControl


class TestHelpers(unittest.TestCase):
    """Test methods in CoreCredentials class."""

    def test_get_qc_id(self):
        """Test get_quality_control function."""
        # Get json dict from test file
        with open("./tests/resources/helpers/quality_control.json", "r") as f:
            qc_dict = json.load(f)

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": qc_dict}
        ]

        qc = get_quality_control(client, _id="123")

        self.assertEqual(
            qc, QualityControl.model_validate_json(json.dumps(qc_dict))
        )

    def test_get_qc_name(self):
        """Test get_quality_control function."""
        # Get json dict from test file
        with open("./tests/resources/helpers/quality_control.json", "r") as f:
            qc_dict = json.load(f)

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": qc_dict}
        ]

        qc = get_quality_control(client, name="123")

        self.assertEqual(
            qc, QualityControl.model_validate_json(json.dumps(qc_dict))
        )

    def test_get_qc_no_identifier(self):
        """Test condition where no name or id is provided"""
        self.assertRaises(ValueError, get_quality_control, MagicMock())

    def test_get_qc_no_record(self):
        """Test that a value error is raised when no record exists."""
        # Get json dict from test file
        client = MagicMock()
        client.retrieve_docdb_records.return_value = []

        self.assertRaises(ValueError, get_quality_control, client, _id="123")

    def test_get_qc_invalid(self):
        """Test that a value error is raised when qc is invalid."""
        # Get json dict from test file
        with open(
            "./tests/resources/helpers/quality_control_invalid.json", "r"
        ) as f:
            qc_dict = json.load(f)

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": qc_dict}
        ]

        self.assertRaises(ValueError, get_quality_control, client, _id="123")

    def test_get_qc_invalid_allowed(self):
        """Test that a dict is returned when we allow invalid."""
        # Get json dict from test file
        with open(
            "./tests/resources/helpers/quality_control_invalid.json", "r"
        ) as f:
            qc_dict = json.load(f)

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": qc_dict}
        ]

        qc = get_quality_control(client, _id="123", allow_invalid=True)

        self.assertEqual(qc, qc_dict)

    def test_get_qc_no_name(self):
        """Test that a value error is raised when no record exists."""
        # Get json dict from test file
        client = MagicMock()
        client.retrieve_docdb_records.return_value = []

        self.assertRaises(ValueError, get_quality_control, client, name="123")


if __name__ == "__main__":
    unittest.main()
