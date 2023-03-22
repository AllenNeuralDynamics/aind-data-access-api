"""Test credentials module"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from aind_data_access_api.credentials import CoreCredentials, EnvVarKeys
from aind_data_access_api.document_store import DocumentStoreCredentials
from aind_data_access_api.rds_tables import RDSCredentials


class TestEnvVarKeys(unittest.TestCase):
    """Tests that the env var keys enums are sound"""

    def test_env_var_keys(self):
        """Test that the string representation is same as value."""

        self.assertEqual(
            EnvVarKeys.DOC_STORE_PASSWORD.value,
            str(EnvVarKeys.DOC_STORE_PASSWORD),
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_USER.value, str(EnvVarKeys.DOC_STORE_USER)
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_HOST.value, str(EnvVarKeys.DOC_STORE_HOST)
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_PORT.value, str(EnvVarKeys.DOC_STORE_PORT)
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_DATABASE.value,
            str(EnvVarKeys.DOC_STORE_DATABASE),
        )
        self.assertEqual(
            EnvVarKeys.RDS_PASSWORD.value,
            str(EnvVarKeys.RDS_PASSWORD),
        )
        self.assertEqual(EnvVarKeys.RDS_USER.value, str(EnvVarKeys.RDS_USER))
        self.assertEqual(EnvVarKeys.RDS_HOST.value, str(EnvVarKeys.RDS_HOST))
        self.assertEqual(EnvVarKeys.RDS_PORT.value, str(EnvVarKeys.RDS_PORT))
        self.assertEqual(
            EnvVarKeys.RDS_DATABASE.value, str(EnvVarKeys.RDS_DATABASE)
        )


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


class TestDocumentStoreCredentials(unittest.TestCase):
    """Test methods in DocumentStoreCredentials class."""

    def test_default_port(self):
        """Tests default port is set correctly"""
        creds = DocumentStoreCredentials(
            username="my_user",
            password="my_password",
            host="my_host",
            database="my_db",
        )

        self.assertEqual(27017, creds.port)

    @patch.dict(
        os.environ,
        {
            "DOC_STORE_USER": "fake_user",
            "DOC_STORE_PASSWORD": "fake_password",
            "DOC_STORE_host": "localhost",
            "DOC_STORE_PORT": "12345",
            "DOC_STORE_DATABASE": "db",
        },
        clear=True,
    )
    def test_credentials_loaded_from_env(self):
        """Tests that the credentials are loaded from env vars correctly"""
        creds = DocumentStoreCredentials()
        self.assertEqual("fake_user", creds.username)
        self.assertEqual("localhost", creds.host)
        self.assertEqual(12345, creds.port)
        self.assertEqual("fake_password", creds.password.get_secret_value())
        self.assertEqual("db", creds.database)

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
        creds1 = DocumentStoreCredentials(aws_secrets_name="abc/def")
        self.assertEqual("user_from_aws", creds1.username)
        self.assertEqual(
            "password_from_aws", creds1.password.get_secret_value()
        )
        self.assertEqual("host_from_aws", creds1.host)
        self.assertEqual(12345, creds1.port)
        self.assertEqual("db_from_aws", creds1.database)

    @patch.dict(
        os.environ,
        {
            "DOC_STORE_USER": "fake_user",
            "DOC_STORE_PASSWORD": "fake_password",
            "DOC_STORE_PORT": "12346",
        },
        clear=True,
    )
    @patch("boto3.client")
    def test_resolve(self, mock_boto_client: MagicMock):
        """Tests mixture of three sources are resolved in order."""
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

        creds1 = DocumentStoreCredentials(aws_secrets_name="abc/def")
        creds2 = DocumentStoreCredentials(
            username="my_user", aws_secrets_name="abc/def"
        )
        creds3 = DocumentStoreCredentials(
            username="my_user", port=5678, aws_secrets_name="abc/def"
        )
        creds4 = DocumentStoreCredentials(host="my_host", database="my_db")

        self.assertEqual("fake_user", creds1.username)
        self.assertEqual("fake_password", creds1.password.get_secret_value())
        self.assertEqual("host_from_aws", creds1.host)
        self.assertEqual(12346, creds1.port)
        self.assertEqual("db_from_aws", creds1.database)

        self.assertEqual("my_user", creds2.username)
        self.assertEqual("fake_password", creds2.password.get_secret_value())
        self.assertEqual("host_from_aws", creds2.host)
        self.assertEqual(12346, creds2.port)
        self.assertEqual("db_from_aws", creds2.database)

        self.assertEqual("my_user", creds3.username)
        self.assertEqual("fake_password", creds3.password.get_secret_value())
        self.assertEqual("host_from_aws", creds3.host)
        self.assertEqual(5678, creds3.port)
        self.assertEqual("db_from_aws", creds3.database)

        self.assertEqual("fake_user", creds4.username)
        self.assertEqual("fake_password", creds4.password.get_secret_value())
        self.assertEqual("my_host", creds4.host)
        self.assertEqual(12346, creds4.port)
        self.assertEqual("my_db", creds4.database)


class TestRDSCredentials(unittest.TestCase):
    """Test methods in DocumentStoreCredentials class."""

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
