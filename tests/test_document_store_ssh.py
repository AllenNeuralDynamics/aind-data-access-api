"""Test document_store_ssh module."""

import os
import unittest
from unittest.mock import MagicMock, patch

from bson import Timestamp
from pymongo import MongoClient

from aind_data_access_api.document_store_ssh import (
    DocumentStoreSSHClient,
    DocumentStoreSSHCredentials,
)

class TestDocumentStoreSSHCredentials(unittest.TestCase):
    """Tests the DocumentStoreSSHCredentials class."""

    def test_defaults(self):
        """Tests default values with class constructor."""
        creds = DocumentStoreSSHCredentials(
            host="doc_store_host",
            username="doc_store_username",
            password="doc_store_password",
            ssh_host="123.456.789.0",
            ssh_username="ssh_username",
            ssh_password="ssh_password",
        )
        self.assertEqual("doc_store_host", creds.host)
        self.assertEqual(27017, creds.port)
        self.assertEqual("doc_store_username", creds.username)
        self.assertEqual("doc_store_password", creds.password.get_secret_value())
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
            "DOC_STORE_HOST": "env_doc_store_host",
            "DOC_STORE_PORT": "123",
            "DOC_STORE_USERNAME": "env_doc_store_username",
            "DOC_STORE_PASSWORD": "env_doc_store_password",
            "DOC_STORE_SSH_HOST": "123.456.789.0",
            "DOC_STORE_SSH_USERNAME": "env_ssh_username",
            "DOC_STORE_SSH_PASSWORD": "env_ssh_password",
        },
        clear=True,
    )
    def test_from_env(self):
        """Tests class constructor with environment variables."""
        creds = DocumentStoreSSHCredentials()
        self.assertEqual("env_doc_store_host", creds.host)
        self.assertEqual(123, creds.port)
        self.assertEqual("env_doc_store_username", creds.username)
        self.assertEqual("env_doc_store_password", creds.password.get_secret_value())
        self.assertEqual("123.456.789.0", creds.ssh_host)
        self.assertEqual("env_ssh_username", creds.ssh_username)
        self.assertEqual("env_ssh_password", creds.ssh_password.get_secret_value())

    @patch("aind_data_access_api.document_store_ssh.get_secret")
    def test_from_secrets_manager(self, mock_get_secret: MagicMock):
        """Tests that the class can be constructed from AWS Secrets Manager."""
        doc_store_secret_name = "/doc/store/secret/name"
        ssh_secret_name = "ssh_secret_name"
        mock_get_secret.side_effect = [
            {
                'admin_secrets': '/admin/secret/name',
                'engine': 'mongo',
                "host": "aws_doc_store_host",
                "password": "aws_doc_store_password",
                "port": 456,
                "username": "aws_doc_store_username",                
            },
            {
                'host': '123.456.789.0',
                'username': 'aws_ssh_username',
                'password': 'aws_ssh_password',
            }
        ]
        creds = DocumentStoreSSHCredentials.from_secrets_manager(
            doc_store_secret_name=doc_store_secret_name,
            ssh_secret_name=ssh_secret_name,
        )
        self.assertEqual("aws_doc_store_host", creds.host)
        self.assertEqual(456, creds.port)
        self.assertEqual("aws_doc_store_username", creds.username)
        self.assertEqual("aws_doc_store_password", creds.password.get_secret_value())
        self.assertEqual("123.456.789.0", creds.ssh_host)
        self.assertEqual("aws_ssh_username", creds.ssh_username)
        self.assertEqual("aws_ssh_password", creds.ssh_password.get_secret_value())


class TestDocumentStoreSSHClient(unittest.TestCase):
    """Tests the DocumentStoreSSHClient class."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up mock credentials for testing"""

        credentials = DocumentStoreSSHCredentials(
            host="doc_store_host",
            username="doc_store_username",
            password="doc_store_password",
            ssh_host="123.456.789.0",
            ssh_username="ssh_username",
            ssh_password="ssh_password",
        )
        cls.credentials = credentials

    def test_init(self):
        """Tests the class constructor."""
        client = DocumentStoreSSHClient(credentials=self.credentials)
        self.assertEqual(self.credentials, client.credentials)
        self.assertEqual("metadata_index", client.database_name)
        self.assertEqual("data_assets", client.collection_name)

    @patch("pymongo.MongoClient.__init__", return_value=None)
    def test_create_mongo_client(self, mock_mongo_client: MagicMock):
        """Tests _create_mongo_client method."""
        client = DocumentStoreSSHClient(credentials=self.credentials)
        client._create_mongo_client()
        mock_mongo_client.assert_called_once_with(
            host="localhost",
            port=27017,
            retryWrites=False,
            directConnection=True,
            username="doc_store_username",
            password="doc_store_password",
            authSource="admin",
            authMechanism="SCRAM-SHA-1",
        )

    @patch("aind_data_access_api.document_store_ssh.SSHTunnelForwarder")
    def test_create_ssh_tunnel2(self, mock_ssh_tunnel: MagicMock):
        """Tests _create_ssh_tunnel method."""
        client = DocumentStoreSSHClient(credentials=self.credentials)
        ssh_tunnel = client._create_ssh_tunnel()
        mock_ssh_tunnel.assert_called_once_with(
            ssh_address_or_host=("123.456.789.0", 22),
            ssh_username="ssh_username",
            ssh_password="ssh_password",
            remote_bind_address=("doc_store_host", 27017),
            local_bind_address=("localhost", 27017),
        )
        self.assertIsInstance(ssh_tunnel, MagicMock)
    
    def test_start_ssh_tunnel(self):
        """Tests _start_ssh_tunnel method."""
        mock_ssh_server = MagicMock()
        mock_ssh_server.is_active = False
        client = DocumentStoreSSHClient(credentials=self.credentials)
        client._ssh_server = mock_ssh_server
        client._start_ssh_tunnel()
        client._ssh_server.start.assert_called_once()
    
    @patch("logging.info")
    def test_start_ssh_tunnel_active(self, mock_log_info: MagicMock):
        """Tests _start_ssh_tunnel method when already active."""
        mock_ssh_server = MagicMock()
        mock_ssh_server.is_active = True
        client = DocumentStoreSSHClient(credentials=self.credentials)
        client._ssh_server = mock_ssh_server
        client._start_ssh_tunnel()
        client._ssh_server.start.assert_not_called()
        mock_log_info.assert_called_once_with("SSH tunnel is already active")

if __name__ == "__main__":
    unittest.main()
