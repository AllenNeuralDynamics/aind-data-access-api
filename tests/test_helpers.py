"""Test helpers module"""

import unittest
import json
from unittest.mock import MagicMock
from aind_data_schema.core.quality_control import QualityControl


class TestHelpers(unittest.TestCase):
    """Test methods in CoreCredentials class."""

    def test_get_quality_control(self):
        """Test get_quality_control function."""
        from aind_data_access_api.helpers import get_quality_control

        # Get json dict from test file
        with open("./tests/resources/helpers/quality_control.json", "r") as f:
            qc_dict = json.load(f)

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [
            {"quality_control": qc_dict}
        ]

        qc = get_quality_control(client, id="123")

        self.assertEqual(
            qc, QualityControl.model_validate_json(json.dumps(qc_dict))
        )


if __name__ == "__main__":
    unittest.main()
