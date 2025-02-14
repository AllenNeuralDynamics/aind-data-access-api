"""Test helpers.data_schema module."""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_access_api.helpers.data_schema import (
    get_quality_control_by_id,
    get_quality_control_by_name,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
TEST_HELPERS_DIR = TEST_DIR / "resources" / "helpers"


class TestHelpersDataSchema(unittest.TestCase):
    """Test methods in data schema."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class by extracting contents from example files."""

        valid_path = TEST_HELPERS_DIR / "quality_control.json"
        with valid_path.open("r") as f:
            cls.example_quality_control = json.load(f)

        invalid_path = TEST_HELPERS_DIR / "quality_control_invalid.json"
        with invalid_path.open("r") as f:
            cls.example_quality_control_invalid = json.load(f)

    @patch(
        "aind_data_schema.core.quality_control."
        "QualityControl.model_validate_json"
    )
    def test_get_qc_id(self, mock_model_validate_json: MagicMock):
        """Test get_quality_control function."""
        mock_model_validate_json.return_value = "mock validated qc"
        # Get json dict from test file
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": self.example_quality_control}
        ]

        qc = get_quality_control_by_id(client, _id="123")

        self.assertEqual(qc, "mock validated qc")
        mock_model_validate_json.assert_called_once_with(
            json.dumps(self.example_quality_control)
        )

    @patch(
        "aind_data_schema.core.quality_control."
        "QualityControl.model_validate_json"
    )
    def test_get_qc_name(self, mock_model_validate_json: MagicMock):
        """Test get_quality_control function."""
        mock_model_validate_json.return_value = "mock validated qc"
        # Get json dict from test file
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": self.example_quality_control}
        ]

        qc = get_quality_control_by_name(client, name="123")

        self.assertEqual(qc, "mock validated qc")
        mock_model_validate_json.assert_called_once_with(
            json.dumps(self.example_quality_control)
        )

    def test_get_qc_no_record(self):
        """Test that a value error is raised when no record exists."""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = []

        self.assertRaises(
            ValueError, get_quality_control_by_id, client, _id="123"
        )

    def test_get_qc_invalid(self):
        """Test that a value error is raised when qc is invalid."""
        # Get json dict from test file

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {
                "_id": "abcd",
                "quality_control": self.example_quality_control_invalid,
            }
        ]

        self.assertRaises(
            ValueError, get_quality_control_by_id, client, _id="123"
        )

    def test_get_qc_invalid_allowed(self):
        """Test that a dict is returned when we allow invalid."""
        # Get json dict from test file
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {
                "_id": "abcd",
                "quality_control": self.example_quality_control_invalid,
            }
        ]

        qc = get_quality_control_by_id(client, _id="123", allow_invalid=True)

        self.assertEqual(qc, self.example_quality_control_invalid)

    def test_get_qc_no_name(self):
        """Test that a value error is raised when no record exists."""
        client = MagicMock()
        client.retrieve_docdb_records.return_value = []

        self.assertRaises(
            ValueError, get_quality_control_by_name, client, name="123"
        )

    def test_get_qc_no_qc(self):
        """Test that a value error is raised when no qc exists."""
        # Get json dict from test file
        client = MagicMock()
        client.retrieve_docdb_records.return_value = [{"_id": "abcd"}]

        self.assertRaises(
            ValueError, get_quality_control_by_id, client, _id="123"
        )

        client.retrieve_docdb_records.return_value = [
            {"_id": "abcd", "quality_control": None}
        ]

        self.assertRaises(
            ValueError, get_quality_control_by_id, client, _id="123"
        )


if __name__ == "__main__":
    unittest.main()
