"""Module to interface with the Document Store"""

import json
import logging
from typing import Iterator, List, Optional

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import SettingsConfigDict
from pymongo import MongoClient, UpdateOne

from aind_data_access_api.credentials import CoreCredentials
from aind_data_access_api.models import DataAssetRecord


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

    def retrieve_data_asset_records(
        self, query: dict = None
    ) -> Iterator[DataAssetRecord]:
        """
        Retrieve data asset records. Will pull all records if query is None.
        Can add a query to filter the records. For example:
        retrieve_data_asset_records(query = {"subject.subject_id":"646253"})
        will retrieve records for that specific subject_id.

        Parameters
        ----------
        query : dict
          A query to add additional filtering. Consult:
          https://pymongo.readthedocs.io/en/stable/tutorial.html
          for additional information.

        Returns
        -------
        Iterator[DataAssetRecord]

        """
        iter_response = self.collection.find(query)
        for response in iter_response:
            yield DataAssetRecord(**response)

    def upsert_one_record(self, data_asset_record: DataAssetRecord) -> None:
        """
        Upsert a single record into DocumentStore.
        Parameters
        ----------
        data_asset_record : DataAssetRecord

        Returns
        -------
        None

        """
        # TODO: Add error handling
        upsert_response = self.collection.update_one(
            {"_id": data_asset_record.id},
            {
                "$set": json.loads(
                    data_asset_record.model_dump_json(by_alias=True)
                )
            },
            upsert=True,
        )
        logging.info(upsert_response)

    def upsert_list_of_records(
        self, data_asset_records: List[DataAssetRecord]
    ) -> None:
        """
        Bulk upsert a list of records into the Document Store
        Parameters
        ----------
        data_asset_records : List[DataAssetRecord]

        Returns
        -------
        None

        """
        # TODO: Add error handling
        operations = [
            UpdateOne(
                {"_id": rec.id},
                {"$set": json.loads(rec.model_dump_json(by_alias=True))},
                upsert=True,
            )
            for rec in data_asset_records
        ]
        bulk_write_response = self.collection.bulk_write(operations)
        logging.info(bulk_write_response)
