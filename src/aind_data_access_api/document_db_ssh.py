from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder

from aind_data_access_api.secrets import get_secret


class DocumentDBSSHCredentials(BaseSettings):
    doc_db_host: str
    doc_db_port: int
    doc_db_username: str
    doc_db_password: SecretStr
    doc_db_db_name: str = "metadata_index"
    doc_db_collection_name: str = "data_assets"

    ssh_host: str
    ssh_port: int = 22
    ssh_username: str
    ssh_password: SecretStr

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
            doc_db_host=docdb_secret["host"],
            doc_db_port=docdb_secret["port"],
            doc_db_username=docdb_secret["username"],
            doc_db_password=SecretStr(docdb_secret["password"]),
            ssh_host=ssh_secret["host"],
            ssh_username=ssh_secret["username"],
            ssh_password=SecretStr(ssh_secret["password"]),
        )

class DocumentDBSSHClient:
    def __init__(self, credentials: DocumentDBSSHCredentials):
        self.credentials = credentials
        self.start_ssh_tunnel()
        self._client = self.create_doc_db_client()
        self.test_connection()
    
    def start_ssh_tunnel(self):
        try:
            self._ssh_server = SSHTunnelForwarder(
                (self.credentials.ssh_host, self.credentials.ssh_port),
                ssh_username=self.credentials.ssh_username,
                ssh_password=self.credentials.ssh_password.get_secret_value(),
                remote_bind_address=(self.credentials.doc_db_host, self.credentials.doc_db_port),
                local_bind_address=('localhost', self.credentials.doc_db_port)
            )
            self._ssh_server.start()
            print(f"SSH tunnel started on localhost:{self.credentials.ssh_port}")
        except Exception as e:
            raise ValueError(f"Failed to start SSH tunnel: {e}")

    def create_doc_db_client(self):
        return MongoClient(
            host="localhost",
            port=self.credentials.doc_db_port,
            retryWrites=False,
            directConnection=True,
            username=self.credentials.doc_db_username,
            password=self.credentials.doc_db_password.get_secret_value(),
            authSource="admin",
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