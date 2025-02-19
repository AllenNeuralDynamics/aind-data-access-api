"""Module to interface with the Document Store"""

from typing import Optional

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import SettingsConfigDict
from pymongo import MongoClient

from aind_data_access_api.credentials import CoreCredentials


# TODO: deprecate this class
class DocumentStoreCredentials(CoreCredentials):
    """Document Store credentials"""

    model_config = SettingsConfigDict(env_prefix="DOC_STORE_")

    # Setting validation aliases for legacy purposes. Allows users
    # to use DOC_STORE_USER in addition to DOC_STORE_USERNAME as env vars
    username: str = Field(
        ...,
        validation_alias=AliasChoices(
            "username", "DOC_STORE_USER", "DOC_STORE_USERNAME"
        ),
    )
    password: SecretStr = Field(...)
    host: str = Field(...)
    port: int = Field(default=27017)
    database: str = Field(...)


# TODO: deprecate this client
class Client:
    """Class to establish a document store client."""

    def __init__(
        self,
        credentials: DocumentStoreCredentials,
        collection_name: Optional[str] = None,
        retry_writes: Optional[bool] = True,
    ):
        """
        Construct a client to interface with a document store.
        Parameters
        ----------
        credentials: CoreCredentials
        collection_name : Optional[str]
        retry_writes : Optional[bool]
          Whether supported write operations executed within the MongoClient
          will be retried once after a network error. Default is True. Set to
          False if writing to AWS DocumentDB.
        """
        self.credentials = credentials
        self.collection_name = collection_name
        self._client = MongoClient(
            credentials.host,
            port=credentials.port,
            username=credentials.username,
            password=credentials.password.get_secret_value(),
            retryWrites=retry_writes,
        )

    @property
    def collection(self):
        """Collection of records in Document Store database to access."""
        db = self._client[self.credentials.database]
        collection = (
            None if self.collection_name is None else db[self.collection_name]
        )
        return collection

    def close(self):
        """Close the client."""
        self._client.close()
