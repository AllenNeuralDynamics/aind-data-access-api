from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder

from aind_data_access_api.secrets import get_secret


class DocumentDBSSHCredentials(BaseSettings):
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
    def from_secrets_manager(cls, doc_db_secret_name: str, ssh_secret_name: str):
        """
        Construct class from aws secrets manager

        Parameters
        ----------
        doc_db_secret_name : str
            The name of the secret in AWS Secrets Manager that contains the
            document db credentials (host, port, username, password).
        ssh_secret_name : str
            The name of the secret in AWS Secrets Manager that contains the
            ssh credentials (host, username, password).
        """
        docdb_secret = get_secret(doc_db_secret_name)
        ssh_secret = get_secret(ssh_secret_name)
        return DocumentDBSSHCredentials(
            **docdb_secret, **{"ssh_" + k: v for k, v in ssh_secret.items()}
        )

class DocumentDBSSHClient:
    def __init__(self, credentials: DocumentDBSSHCredentials):
        self.credentials = credentials
        self._client = self.create_doc_db_client()
        self._ssh_server = self.create_ssh_tunnel()
        self.start_ssh_tunnel()
        self.test_connection()

    def create_ssh_tunnel(self):
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
        if not self._ssh_server.is_active:
            self._ssh_server.start()
        else:
            logging.info("SSH tunnel is already active")

    def create_doc_db_client(self):
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
    
    def test_connection(self):
        try:
            server_info = self._client.server_info()
            print(server_info)
        except Exception as e:
            raise ValueError(f"Failed to connect to document db: {e}")
    
    def close(self):
        self._client.close()
        self._ssh_server.stop()
        pass