"""Test helpers module"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from aind_data_access_api.credentials import CoreCredentials
from aind_data_access_api.document_store import DocumentStoreCredentials
from aind_data_access_api.rds_tables import RDSCredentials


class TestHelpers(unittest.TestCase):
    """Test methods in CoreCredentials class."""

    def test_get_quality_control(self):
        """Test get_quality_control function."""
        from aind_data_access_api.helpers import get_quality_control

        # Get json dict from test file
        with open("./resources/helpers/quality_control.json", "r") as f:
            qc_str = f.read()

        client = MagicMock()
        client.retrieve_docdb_records.return_value = [qc_str]

        qc = get_quality_control(client, id="123")

        # self.assertEqual(qc, )

if __name__ == "__main__":
    unittest.main()
