import pymongo
from pymongo import UpdateOne
from typing import Optional, List
from aind_data_access_api.credentials import DocumentStoreCredentials
from aind_data_access_api.models import DataAssetRecord
import logging


class Client:
    def __init__(
        self,
        credentials: Optional[DocumentStoreCredentials] = None,
        _password: Optional[str] = None,
        _user: Optional[str] = None,
        _host: Optional[str] = None,
        db_name: str = None,
        collection_name: str = None,
    ):
        if credentials is None:
            self.credentials = DocumentStoreCredentials(
                password=_password, user=_user, host=_host
            )
        else:
            self.credentials = credentials
        self.db_name = db_name
        self.collection_name = collection_name
        self._client = pymongo.MongoClient(self._url)

    @property
    def _url(self):
        mongodb_url = (
            f"mongodb://{self.credentials.user}:"
            f"{self.credentials.password}@"
            f"{self.credentials.host}"
        )
        return mongodb_url

    @property
    def db(self):
        db = None if self.db_name is None else self._client[self.db_name]
        return db

    @property
    def collection(self):
        collection = (
            None
            if self.collection_name is None
            else self.db[self.collection_name]
        )
        return collection

    def close(self):
        self._client.close()

    def retrieve_data_asset_records(self, query=None) -> None:
        iter_response = self.collection.find(query)
        for response in iter_response:
            yield DataAssetRecord(**response)

    def upsert_one_record(self, data_asset_record: DataAssetRecord) -> None:
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
        # TODO: Add error handling
        operations = [
            UpdateOne(
                {"_id": rec.id}, {"$set": rec.dict(by_alias=True)}, upsert=True
            )
            for rec in data_asset_records
        ]
        bulk_write_response = self.collection.bulk_write(operations)
        logging.info(bulk_write_response)
