"""Module to interface with the Document Database using SSH tunneling."""

import logging

from pydantic import Field, SecretStr
from pydantic_settings import SettingsConfigDict
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder

from aind_data_access_api.credentials import CoreCredentials
from aind_data_access_api.secrets import get_secret


class DocumentDbSSHCredentials(CoreCredentials):
    """Document Store credentials with SSH tunneling."""

    model_config = SettingsConfigDict(env_prefix="DOC_DB_", extra="ignore")

    host: str = Field(..., description="The host of the document database")
    port: int = Field(
        default=27017, description="The port of the document database"
    )
    username: str = Field(..., description="The username for authentication")
    password: SecretStr = Field(
        ..., description="The password for authentication"
    )
    database: str = Field(
        default="metadata_index", description="The name of the database"
    )
    collection: str = Field(
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
            The name of the secret that contains the document store credentials
            (host, port, username, password).
        ssh_secret_name : str
            The name of the secret that contains the ssh credentials
            (host, username, password).
        """
        docdb_secret = get_secret(doc_db_secret_name)
        ssh_secret = get_secret(ssh_secret_name)
        return DocumentDbSSHCredentials(
            **docdb_secret, **{"ssh_" + k: v for k, v in ssh_secret.items()}
        )


class DocumentDbSSHClient:
    """Class to establish a Document Store client with SSH tunneling."""

    def __init__(self, credentials: DocumentDbSSHCredentials):
        """
        Construct a client to interface with a Document Database.

        Parameters
        ----------
        credentials : DocumentDbSSHCredentials
        """
        self.credentials = credentials
        self.database_name = credentials.database
        self.collection_name = credentials.collection

    @property
    def collection(self):
        """Collection of metadata records in Document Database."""
        db = self._client[self.database_name]
        collection = db[self.collection_name]
        return collection

    def _create_mongo_client(self):
        """
        Create a MongoClient to connect to the Document Store.
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

    def _create_ssh_tunnel(self):
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

    def start(self):
        """Start the client and SSH tunnel."""
        self._client = self._create_mongo_client()
        self._ssh_server = self._create_ssh_tunnel()
        self._ssh_server.start()
        server_info = self._client.server_info()
        logging.info(server_info)
        logging.info(
            f"Connected to {self.credentials.host}:{self.credentials.port} as "
            f"{self.credentials.username}"
        )

    def close(self):
        """Close the client and SSH tunnel."""
        self._client.close()
        self._ssh_server.stop()
        logging.info("DocDB SSH session closed.")

    def __enter__(self):
        """Enter the context manager."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        self.close()
