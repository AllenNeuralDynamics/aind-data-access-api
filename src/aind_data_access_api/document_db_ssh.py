"""Module to interface with the Document Database using SSH tunneling."""

import logging

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder

from aind_data_access_api.secrets import get_secret


class DocumentDBSSHCredentials(BaseSettings):
    """Document Database credentials with SSH tunneling."""

    model_config = SettingsConfigDict(
        env_prefix="DOC_DB_", env_file=".env", extra="ignore"
    )

    host: str = Field(..., description="The host of the document database")
    port: int = Field(
        default=27017, description="The port of the document database"
    )
    username: str = Field(..., description="The username for authentication")
    password: SecretStr = Field(
        ..., description="The password for authentication"
    )
    db_name: str = Field(
        default="metadata_index", description="The name of the database"
    )
    collection_name: str = Field(
        default="data_assets", description="The name of the collection"
    )
    ssh_local_bind_address: str = Field(
        default="localhost",
        description="The local bind address for SSH tunneling",
    )
    ssh_host: str = Field(..., description="The host of the SSH server")
    ssh_port: int = Field(default=22, description="The port of the SSH server")
    ssh_username: str = Field(
        ..., description="The username for SSH authentication"
    )
    ssh_password: SecretStr = Field(
        ..., description="The password for SSH authentication"
    )

    @classmethod
    def from_secrets_manager(
        cls, doc_db_secret_name: str, ssh_secret_name: str
    ):
        """
        Construct class from AWS Secrets Manager

        Parameters
        ----------
        doc_db_secret_name : str
            The name of the secret that contains the document db credentials
            (host, port, username, password).
        ssh_secret_name : str
            The name of the secret that contains the ssh credentials
            (host, username, password).
        """
        docdb_secret = get_secret(doc_db_secret_name)
        ssh_secret = get_secret(ssh_secret_name)
        return DocumentDBSSHCredentials(
            **docdb_secret, **{"ssh_" + k: v for k, v in ssh_secret.items()}
        )


class DocumentDBSSHClient:
    """Class to establish a Document Database client with SSH tunneling."""

    def __init__(self, credentials: DocumentDBSSHCredentials):
        """
        Construct a client to interface with a Document Database.

        Parameters
        ----------
        credentials : DocumentDBSSHCredentials
        """
        self.credentials = credentials
        self.db_name = credentials.db_name
        self.collection_name = credentials.collection_name
        self._client = self.create_doc_db_client()
        self._ssh_server = self.create_ssh_tunnel()

    @property
    def collection(self):
        """Collection of metadata records in Document Database."""
        db = self._client[self.db_name]
        collection = db[self.collection_name]
        return collection

    def create_doc_db_client(self):
        """
        Create a MongoClient to connect to the Document Database.
        Uses retryWrites=False to enable writing to AWS DocumentDB.
        Uses authMechanism="SCRAM-SHA-1" for complex usernames.
        """
        return MongoClient(
            host=self.credentials.ssh_local_bind_address,
            port=self.credentials.port,
            retryWrites=False,
            directConnection=True,
            username=self.credentials.username,
            password=self.credentials.password.get_secret_value(),
            authSource="admin",
            authMechanism="SCRAM-SHA-1",
        )

    def create_ssh_tunnel(self):
        """Create an SSH tunnel to the Document Database."""
        return SSHTunnelForwarder(
            ssh_address_or_host=(
                self.credentials.ssh_host,
                self.credentials.ssh_port,
            ),
            ssh_username=self.credentials.ssh_username,
            ssh_password=self.credentials.ssh_password.get_secret_value(),
            remote_bind_address=(self.credentials.host, self.credentials.port),
            local_bind_address=(
                self.credentials.ssh_local_bind_address,
                self.credentials.port,
            ),
        )

    def start_ssh_tunnel(self):
        """Start the SSH tunnel."""
        if not self._ssh_server.is_active:
            self._ssh_server.start()
        else:
            logging.info("SSH tunnel is already active")

    def test_connection(self):
        """Test the connection to the Document Database."""
        server_info = self._client.server_info()
        logging.info(server_info)
        collections = self._client.list_database_names()
        if self.db_name not in collections:
            raise ValueError(f"Database {self.db_name} not found")
        if (
            self.collection_name
            not in self._client[self.db_name].list_collection_names()
        ):
            raise ValueError(f"Collection {self.collection_name} not found")
        logging.info(
            f"Connected to {self.credentials.host}:{self.credentials.port} as {self.credentials.username}"
        )

    def close(self):
        """Close the DocumentDB client and the SSH tunnel."""
        self._client.close()
        self._ssh_server.stop()
        logging.info("DocDB SSH session closed.")

    def __enter__(self):
        self.start_ssh_tunnel()
        self.test_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
