"""Test helpers.data_schema module."""

import json
import os
import unittest
import pandas as pd
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from aind_data_schema.core.quality_control import (
    QualityControl,
    QCEvaluation,
    QCMetric,
    QCStatus,
    Status,
    Stage,
)
from aind_data_schema_models.modalities import Modality

from aind_data_access_api.helpers.data_schema import (
    get_quality_control_by_id,
    get_quality_control_by_name,
    get_quality_control_value_df,
    get_quality_control_status_df,
    get_quality_control_by_names,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
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

    @patch(
        "aind_data_access_api.helpers.data_schema.get_quality_control_by_names"
    )
    def test_get_qc_value_df(
        self, mock_get_quality_control_by_names: MagicMock
    ):
        """Test that a dataframe is correctly returned"""

        status = QCStatus(
            evaluator="Dan",
            status=Status.PASS,
            timestamp=datetime.now(),
        )
        metric0 = QCMetric(
            name="Metric0",
            value=0,
            status_history=[
                status,
            ],
        )

        eval = QCEvaluation(
            name="Evaluation0",
            modality=Modality.ECEPHYS,
            stage=Stage.RAW,
            metrics=[metric0],
        )

        mock_get_quality_control_by_names.return_value = [
            QualityControl(
                evaluations=[eval],
            )
        ]

        client = MagicMock()

        test_df = pd.DataFrame(
            {
                "name": ["fake_name"],
                "Evaluation0_Metric0": [0],
            }
        )

        qc_df = get_quality_control_value_df(client, ["fake_name"])

        pd.testing.assert_frame_equal(test_df, qc_df)

    @patch(
        "aind_data_access_api.helpers.data_schema.get_quality_control_by_names"
    )
    def test_get_qc_status_df(
        self, mock_get_quality_control_by_names: MagicMock
    ):
        """Test that a dataframe is correctly returned"""

        status = QCStatus(
            evaluator="Dan",
            status=Status.PASS,
            timestamp=datetime.now(),
        )
        metric0 = QCMetric(
            name="Metric0",
            value=0,
            status_history=[
                status,
            ],
        )

        eval = QCEvaluation(
            name="Evaluation0",
            modality=Modality.ECEPHYS,
            stage=Stage.RAW,
            metrics=[metric0],
        )

        mock_get_quality_control_by_names.return_value = [
            QualityControl(
                evaluations=[eval],
            )
        ]

        client = MagicMock()

        test_df = pd.DataFrame(
            {
                "name": ["fake_name"],
                "Evaluation0_Metric0": [Status.PASS],
            }
        )

        qc_df = get_quality_control_status_df(client, ["fake_name"])

        pd.testing.assert_frame_equal(test_df, qc_df)

    @patch(
        "aind_data_access_api.helpers.data_schema.fetch_records_by_filter_list"
    )
    def test_get_quality_control_by_names_valid(self, mock_fetch):
        """Test retrieving valid QualityControl objects."""
        mock_client = MagicMock()

        mock_records = [
            {"quality_control": self.example_quality_control.copy()},
            {"quality_control": self.example_quality_control.copy()},
        ]
        mock_fetch.return_value = mock_records

        result = get_quality_control_by_names(
            client=mock_client,
            names=["name1", "name2"],
            allow_invalid=False,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].evaluations[0].name, "Drift map")
        self.assertEqual(result[1].evaluations[0].name, "Drift map")
        mock_fetch.assert_called_once_with(
            mock_client,
            filter_key="name",
            filter_values=["name1", "name2"],
            projection={"quality_control": 1},
        )

    @patch(
        (
            "aind_data_access_api.helpers.data_schema."
            "fetch_records_by_filter_list"
        ),
        name="mock_fetch",
    )
    def test_get_quality_control_by_names_invalid(self, mock_fetch):
        """Test retrieving invalid QualityControl objects."""
        mock_client = MagicMock()
        mock_records = [
            {"quality_control": {"invalid_field": "invalid_value"}},
            {"quality_control": {"invalid_field": "another_invalid_value"}},
        ]
        mock_fetch.return_value = mock_records

        result = get_quality_control_by_names(
            client=mock_client,
            names=["name1", "name2"],
            allow_invalid=True,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["invalid_field"], "invalid_value")
        self.assertEqual(result[1]["invalid_field"], "another_invalid_value")
        mock_fetch.assert_called_once_with(
            mock_client,
            filter_key="name",
            filter_values=["name1", "name2"],
            projection={"quality_control": 1},
        )

    @patch(
        "aind_data_access_api.helpers.data_schema.fetch_records_by_filter_list"
    )
    def test_get_quality_control_by_names_no_records(self, mock_fetch):
        """Test when no records are found."""
        mock_client = MagicMock()
        mock_fetch.return_value = []

        result = get_quality_control_by_names(
            client=mock_client,
            names=["name1", "name2"],
            allow_invalid=False,
        )

        self.assertEqual(result, [])
        mock_fetch.assert_called_once_with(
            mock_client,
            filter_key="name",
            filter_values=["name1", "name2"],
            projection={"quality_control": 1},
        )


if __name__ == "__main__":
    unittest.main()
