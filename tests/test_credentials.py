"""Test credentials module"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from aind_data_access_api.credentials import CoreCredentials
from aind_data_access_api.rds_tables import RDSCredentials


class TestCoreCredentials(unittest.TestCase):
    """Test methods in CoreCredentials class."""

    @patch("boto3.client")
    def test_pull_from_aws(self, mock_boto_client: MagicMock):
        """Tests that creds are set correctly from aws secrets manager"""
        example_response = json.dumps(
            {
                "username": "user_from_aws",
                "password": "password_from_aws",
                "host": "host_from_aws",
                "port": 12345,
                "database": "db_from_aws",
            }
        )

        mock_boto_client.return_value.get_secret_value.return_value = {
            "SecretString": example_response
        }

        creds1 = CoreCredentials(aws_secrets_name="abc/def")
        creds2 = CoreCredentials(host="my_host", aws_secrets_name="abc/def")
        self.assertEqual("user_from_aws", creds1.username)
        self.assertEqual(
            "password_from_aws", creds1.password.get_secret_value()
        )
        self.assertEqual("host_from_aws", creds1.host)
        self.assertEqual(12345, creds1.port)
        self.assertEqual("db_from_aws", creds1.database)
        self.assertEqual("user_from_aws", creds2.username)
        self.assertEqual(
            "password_from_aws", creds2.password.get_secret_value()
        )
        self.assertEqual("my_host", creds2.host)
        self.assertEqual(12345, creds2.port)
        self.assertEqual("db_from_aws", creds2.database)


class TestRDSCredentials(unittest.TestCase):
    """Test methods in RDSCredentials class."""

    def test_default_port(self):
        """Tests default port is set correctly"""
        creds = RDSCredentials(
            username="my_user",
            password="my_password",
            host="my_host",
            database="my_db",
        )

        self.assertEqual(5432, creds.port)

    @patch.dict(
        os.environ,
        {
            "RDS_USER": "fake_user",
            "RDS_PASSWORD": "fake_password",
            "RDS_host": "localhost",
            "RDS_PORT": "12345",
            "RDS_DATABASE": "db",
        },
        clear=True,
    )
    def test_credentials_loaded_from_env(self):
        """Tests that the credentials are loaded from env vars correctly"""
        creds = RDSCredentials()
        self.assertEqual("fake_user", creds.username)
        self.assertEqual("localhost", creds.host)
        self.assertEqual(12345, creds.port)
        self.assertEqual("fake_password", creds.password.get_secret_value())
        self.assertEqual("db", creds.database)


if __name__ == "__main__":
    unittest.main()
