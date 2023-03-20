"""Test models module."""

import unittest
from datetime import datetime

from aind_data_access_api.models import DataAssetRecord


class TestDataAssetRecord(unittest.TestCase):
    """Test methods in DataAssetRecord class."""

    def test_data_asset_record(self):
        """Test field names are mapped correctly."""
        data_asset_record = DataAssetRecord(
            _id="abc-123",
            _name="modal_00000_2000-10-10_10-10-10",
            _created=datetime(2000, 10, 10, 10, 10, 10),
            _location="some_url",
            subject={"subject_id": "00000", "sex": "Female"},
        )

        self.assertEqual(data_asset_record._name, data_asset_record.name)
        self.assertEqual(data_asset_record._id, data_asset_record.id)
        self.assertEqual(
            data_asset_record._location, data_asset_record.location
        )
        self.assertEqual(data_asset_record._created, data_asset_record.created)
        self.assertEqual(
            data_asset_record.subject, {"subject_id": "00000", "sex": "Female"}
        )


if __name__ == "__main__":
    unittest.main()
