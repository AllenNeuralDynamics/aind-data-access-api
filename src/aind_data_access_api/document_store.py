"""Module to interface with the Document Store"""

import logging
from typing import Iterator, List, Optional

from pymongo import MongoClient, UpdateOne

from aind_data_access_api.credentials import DocumentStoreCredentials
from aind_data_access_api.models import DataAssetRecord


class Client:
    """Class to establish a document store client."""

    def __init__(
        self,
        _password: Optional[str] = None,
        _user: Optional[str] = None,
        _host: Optional[str] = None,
        _port: Optional[int] = 27017,
        _database: Optional[str] = None,
        collection_name: Optional[str] = None,
    ):
        """
        Construct a client to interface with a document store.
        Parameters
        ----------
        _password : Optional[str]
        _user : Optional[str]
        _host : Optional[str]
        _port : Optional[int]
        _database : Optional[str]
        collection_name : Optional[str]
        """
        self.credentials = DocumentStoreCredentials(
            password=_password,
            user=_user,
            host=_host,
            port=_port,
            database=_database,
        )
        self.collection_name = collection_name
        self._client = MongoClient(
            _host, port=_port, username=_user, password=_password
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
            {"$set": data_asset_record.dict(by_alias=True)},
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
                {"_id": rec.id}, {"$set": rec.dict(by_alias=True)}, upsert=True
            )
            for rec in data_asset_records
        ]
        bulk_write_response = self.collection.bulk_write(operations)
        logging.info(bulk_write_response)
