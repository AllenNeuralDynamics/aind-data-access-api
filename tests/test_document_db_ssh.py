"""Test document_db_ssh module."""

import os
import unittest
from unittest.mock import MagicMock, call, patch

from bson import Timestamp

from aind_data_access_api.document_db_ssh import (
    DocumentDbSSHClient,
    DocumentDbSSHCredentials,
)


class TestDocumentDbSSHCredentials(unittest.TestCase):
    """Tests the DocumentDbSSHCredentials class."""

    def test_defaults(self):
        """Tests default values with class constructor."""
        creds = DocumentDbSSHCredentials(
            host="doc_db_host",
            username="doc_db_username",
            password="doc_db_password",
            ssh_host="123.456.789.0",
            ssh_username="ssh_username",
            ssh_password="ssh_password",
        )
        self.assertEqual("doc_db_host", creds.host)
        self.assertEqual(27017, creds.port)
        self.assertEqual("doc_db_username", creds.username)
        self.assertEqual("doc_db_password", creds.password.get_secret_value())
        self.assertEqual("metadata_index", creds.database)
        self.assertEqual("data_assets", creds.collection)
        self.assertEqual("localhost", creds.ssh_local_bind_address)
        self.assertEqual("123.456.789.0", creds.ssh_host)
        self.assertEqual(22, creds.ssh_port)
        self.assertEqual("ssh_username", creds.ssh_username)
        self.assertEqual("ssh_password", creds.ssh_password.get_secret_value())

    @patch.dict(
        os.environ,
        {
            "DOC_DB_HOST": "env_doc_db_host",
            "DOC_DB_PORT": "123",
            "DOC_DB_USERNAME": "env_doc_db_username",
            "DOC_DB_PASSWORD": "env_doc_db_password",
            "DOC_DB_SSH_HOST": "123.456.789.0",
            "DOC_DB_SSH_USERNAME": "env_ssh_username",
            "DOC_DB_SSH_PASSWORD": "env_ssh_password",
        },
        clear=True,
    )
    def test_from_env(self):
        """Tests class constructor with environment variables."""
        creds = DocumentDbSSHCredentials()
        self.assertEqual("env_doc_db_host", creds.host)
        self.assertEqual(123, creds.port)
        self.assertEqual("env_doc_db_username", creds.username)
        self.assertEqual(
            "env_doc_db_password", creds.password.get_secret_value()
        )
        self.assertEqual("123.456.789.0", creds.ssh_host)
        self.assertEqual("env_ssh_username", creds.ssh_username)
        self.assertEqual(
            "env_ssh_password", creds.ssh_password.get_secret_value()
        )

    @patch("aind_data_access_api.document_db_ssh.get_secret")
    def test_from_secrets_manager(self, mock_get_secret: MagicMock):
        """Tests that the class can be constructed from AWS Secrets Manager."""
        doc_db_secret_name = "/doc/store/secret/name"
        ssh_secret_name = "ssh_secret_name"
        mock_get_secret.side_effect = [
            {
                "admin_secrets": "/admin/secret/name",
                "engine": "mongo",
                "host": "aws_doc_db_host",
                "password": "aws_doc_db_password",
                "port": 456,
                "username": "aws_doc_db_username",
            },
            {
                "host": "123.456.789.0",
                "username": "aws_ssh_username",
                "password": "aws_ssh_password",
            },
        ]
        creds = DocumentDbSSHCredentials.from_secrets_manager(
            doc_db_secret_name=doc_db_secret_name,
            ssh_secret_name=ssh_secret_name,
        )
        self.assertEqual("aws_doc_db_host", creds.host)
        self.assertEqual(456, creds.port)
        self.assertEqual("aws_doc_db_username", creds.username)
        self.assertEqual(
            "aws_doc_db_password", creds.password.get_secret_value()
        )
        self.assertEqual("123.456.789.0", creds.ssh_host)
        self.assertEqual("aws_ssh_username", creds.ssh_username)
        self.assertEqual(
            "aws_ssh_password", creds.ssh_password.get_secret_value()
        )


class TestDocumentDbSSHClient(unittest.TestCase):
    """Tests the DocumentDbSSHClient class."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up mock credentials for testing"""
        credentials = DocumentDbSSHCredentials(
            host="doc_db_host",
            username="doc_db_username",
            password="doc_db_password",
            ssh_host="123.456.789.0",
            ssh_username="ssh_username",
            ssh_password="ssh_password",
        )
        example_server_info = {
            "version": "5.0.0",
            "versionArray": [5, 0, 0, 0],
            "bits": 64,
            "maxBsonObjectSize": 16777216,
            "ok": 1.0,
            "operationTime": Timestamp(1718212689, 1),
        }
        cls.credentials = credentials
        cls.example_server_info = example_server_info

    def test_init(self):
        """Tests the class constructor."""
        doc_db_client = DocumentDbSSHClient(credentials=self.credentials)
        self.assertEqual(self.credentials, doc_db_client.credentials)
        self.assertEqual("metadata_index", doc_db_client.database_name)
        self.assertEqual("data_assets", doc_db_client.collection_name)

    @patch("aind_data_access_api.document_db_ssh.SSHTunnelForwarder")
    @patch("aind_data_access_api.document_db_ssh.MongoClient")
    @patch("logging.info")
    def test_start(
        self,
        mock_log_info: MagicMock,
        mock_create_mongo_client: MagicMock,
        mock_create_ssh_tunnel: MagicMock,
    ):
        """Tests start method."""
        mock_ssh_tunnel = MagicMock(is_active=False)
        mock_create_ssh_tunnel.return_value = mock_ssh_tunnel
        mock_create_mongo_client.return_value = MagicMock(
            server_info=MagicMock(return_value=self.example_server_info),
        )
        doc_db_client = DocumentDbSSHClient(credentials=self.credentials)
        doc_db_client.start()
        mock_create_mongo_client.assert_called_once_with(
            host="localhost",
            port=27017,
            retryWrites=False,
            directConnection=True,
            username="doc_db_username",
            password="doc_db_password",
            authSource="admin",
            authMechanism="SCRAM-SHA-1",
        )
        mock_create_ssh_tunnel.assert_called_once_with(
            ssh_address_or_host=("123.456.789.0", 22),
            ssh_username="ssh_username",
            ssh_password="ssh_password",
            remote_bind_address=("doc_db_host", 27017),
            local_bind_address=("localhost", 27017),
        )
        mock_ssh_tunnel.start.assert_called_once()
        mock_log_info.assert_has_calls(
            [
                call(self.example_server_info),
                call("Connected to doc_db_host:27017 as doc_db_username"),
            ]
        )

    @patch("logging.info")
    def test_close(self, mock_log_info: MagicMock):
        """Tests close method."""
        mock_ssh_tunnel = MagicMock()
        mock_mongo_client = MagicMock()
        doc_db_client = DocumentDbSSHClient(credentials=self.credentials)
        doc_db_client._ssh_server = mock_ssh_tunnel
        doc_db_client._client = mock_mongo_client
        doc_db_client.close()
        mock_ssh_tunnel.stop.assert_called_once()
        mock_mongo_client.close.assert_called_once()
        mock_log_info.assert_called_once_with("DocDB SSH session closed.")

    @patch("aind_data_access_api.document_db_ssh.SSHTunnelForwarder")
    @patch("aind_data_access_api.document_db_ssh.MongoClient")
    @patch("logging.info")
    def test_context_manager(
        self,
        mock_log_info: MagicMock,
        mock_create_mongo_client: MagicMock,
        mock_create_ssh_tunnel: MagicMock,
    ):
        """Tests using DocumentDbSSHClient in context"""
        mock_ssh_tunnel = MagicMock(is_active=False)
        mock_collection = MagicMock()
        mock_database = MagicMock(
            __getitem__=MagicMock(return_value=mock_collection)
        )
        mock_mongo_client = MagicMock(
            server_info=MagicMock(return_value=self.example_server_info),
            __getitem__=MagicMock(return_value=mock_database),
        )
        mock_create_ssh_tunnel.return_value = mock_ssh_tunnel
        mock_create_mongo_client.return_value = mock_mongo_client

        with DocumentDbSSHClient(
            credentials=self.credentials
        ) as doc_db_client:
            doc_db_client.collection.count_documents({})
        # assert connections are created
        mock_create_mongo_client.assert_called_once_with(
            host="localhost",
            port=27017,
            retryWrites=False,
            directConnection=True,
            username="doc_db_username",
            password="doc_db_password",
            authSource="admin",
            authMechanism="SCRAM-SHA-1",
        )
        mock_create_ssh_tunnel.assert_called_once_with(
            ssh_address_or_host=("123.456.789.0", 22),
            ssh_username="ssh_username",
            ssh_password="ssh_password",
            remote_bind_address=("doc_db_host", 27017),
            local_bind_address=("localhost", 27017),
        )
        mock_ssh_tunnel.start.assert_called_once()
        # assert correct database and collection are accessed
        mock_mongo_client.__getitem__.assert_called_once_with("metadata_index")
        mock_database.__getitem__.assert_called_once_with("data_assets")
        mock_collection.count_documents.assert_called_once_with({})
        # assert connections are closed
        mock_ssh_tunnel.stop.assert_called_once()
        mock_mongo_client.close.assert_called_once()
        mock_log_info.assert_has_calls(
            [
                call(self.example_server_info),
                call("Connected to doc_db_host:27017 as doc_db_username"),
                call("DocDB SSH session closed."),
            ]
        )


if __name__ == "__main__":
    unittest.main()
