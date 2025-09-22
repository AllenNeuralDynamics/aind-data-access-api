"""Module to interface with the DocumentDB"""

import json
import logging
from sys import getsizeof
from typing import Any, Dict, List, Optional

import boto3
from boto3.session import Session as BotoSession
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from requests import Response, Session


class Client:
    """Class to create client to interface with DocumentDB via a REST api"""

    def __init__(
        self,
        host: str,
        database: str,
        collection: str,
        version: str = "v1",
        boto: Optional[BotoSession] = None,
        session: Optional[Session] = None,
    ):
        """
        Instantiate a Client.
        Parameters
        ----------
        host : str
        database : str
        collection : str
        version : str
        boto : Optional[BotoSession]
        session : Optional[Session]
        """
        self.host = host.strip("/")
        self.database = database
        self.collection = collection
        self.version = version
        self._boto = boto
        self._session = session

    @property
    def _base_url(self) -> str:
        """Construct base url to interface with a collection in a database."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}"
        )

    @property
    def _count_url(self) -> str:
        """Url to count records."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/count_documents"
        )

    @property
    def _find_url(self) -> str:
        """Url to find records."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/find"
        )

    @property
    def _aggregate_url(self) -> str:
        """Url to aggregate records."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/aggregate"
        )

    @property
    def _insert_one_url(self) -> str:
        """Url to insert one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/insert_one"
        )

    @property
    def _update_one_url(self) -> str:
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/update_one"
        )

    @property
    def _delete_one_url(self) -> str:
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/delete_one"
        )

    @property
    def _delete_many_url(self) -> str:
        """Url to update one record"""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/delete_many"
        )

    @property
    def _bulk_write_url(self) -> str:
        """Url to bulk write many records."""
        return (
            f"https://{self.host}/{self.version}/{self.database}/"
            f"{self.collection}/bulk_write"
        )

    @property
    def boto(self) -> BotoSession:
        """Boto3 session"""
        if self._boto is None:
            self._boto = boto3.session.Session()
        return self._boto

    @property
    def session(self) -> Session:
        """Requests session"""
        if self._session is None:
            self._session = Session()
        return self._session

    def _signed_request(
        self,
        url: str,
        method: str,
        params: Optional[dict] = None,
        data: Optional[str] = None,
    ) -> AWSRequest:
        """Create a signed request to the DocumentDB REST api.
        Permissions are managed through AWS."""
        aws_request = AWSRequest(
            url=url,
            method=method,
            data=data,
            params=params,
            headers={"Content-Type": "application/json"},
        )
        SigV4Auth(
            self.boto.get_credentials(),
            "execute-api",
            self.boto.region_name,
        ).add_auth(aws_request)
        return aws_request

    def _count_records(self, filter_query: Optional[dict] = None):
        """
        Count the number of records in a collection.
        Parameters
        ----------
        filter_query : Optional[dict]
          If passed, will return the number of records and number of records
          returned by the filter query.

        Returns
        -------
        dict
          Has keys {"total_record_count": int, "filtered_record_count": int}

        """
        params = (
            {"filter": json.dumps(filter_query)}
            if filter_query is not None
            else None
        )
        response = self.session.get(self._count_url, params=params)
        response.raise_for_status()
        response_body = response.json()
        return response_body

    def _find_records(
        self,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        skip: int = 0,
    ) -> List[dict]:
        """
        Retrieve records from collection. May return a smaller set of records
        if requested records exceed the max payload size of the API Gateway.

        Parameters
        ----------
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        skip : int
          Skip this amount of records in index when applying search.

        Returns
        -------
        List[dict]
          The list of records returned from the DocumentDB.

        """
        params = {"limit": str(limit), "skip": str(skip)}
        if filter_query is not None:
            params["filter"] = json.dumps(filter_query)
        if projection is not None:
            params["projection"] = json.dumps(projection)
        if sort is not None:
            params["sort"] = json.dumps(sort)

        response = self.session.get(self._find_url, params=params)
        response.raise_for_status()
        response_body = response.json()
        return response_body

    def _get_records(
        self,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        skip: int = 0,
    ) -> List[dict]:
        """
        Retrieve records from collection. May raise HTTP 413 error if
        requested records exceed the max payload size of the API Gateway.

        Parameters
        ----------
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        skip : int
          Skip this amount of records in index when applying search.

        Returns
        -------
        List[dict]
          The list of records returned from the DocumentDB.

        """
        params = {"limit": str(limit), "skip": str(skip)}
        if filter_query is not None:
            params["filter"] = json.dumps(filter_query)
        if projection is not None:
            params["projection"] = json.dumps(projection)
        if sort is not None:
            params["sort"] = json.dumps(sort)

        response = self.session.get(self._base_url, params=params)
        response.raise_for_status()
        response_body = response.json()
        return response_body

    def _aggregate_records(self, pipeline: List[dict]) -> List[dict]:
        """Aggregate records from collection using an aggregation pipeline."""
        # Do not need to sign request since API supports readonly aggregations
        response = self.session.post(url=self._aggregate_url, json=pipeline)
        response.raise_for_status()
        response_body = response.json()
        return response_body

    def _insert_one_record(self, record: dict) -> Response:
        """Insert a single record into the collection."""
        data = json.dumps(record)
        signed_header = self._signed_request(
            method="POST", url=self._insert_one_url, data=data
        )
        response = self.session.post(
            url=self._insert_one_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        response.raise_for_status()
        return response

    def _upsert_one_record(
        self, record_filter: dict, update: dict
    ) -> Response:
        """Upsert a single record into the collection."""
        data = json.dumps(
            {"filter": record_filter, "update": update, "upsert": "True"}
        )
        signed_header = self._signed_request(
            method="POST", url=self._update_one_url, data=data
        )
        response = self.session.post(
            url=self._update_one_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        response.raise_for_status()
        return response

    def _delete_one_record(self, record_filter: dict) -> Response:
        """Upsert a single record into the collection."""
        data = json.dumps({"filter": record_filter})
        signed_header = self._signed_request(
            method="DELETE", url=self._delete_one_url, data=data
        )
        response = self.session.delete(
            url=self._delete_one_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        return response

    def _delete_many_records(self, record_filter: dict) -> Response:
        """Upsert a single record into the collection."""
        data = json.dumps({"filter": record_filter})
        signed_header = self._signed_request(
            method="DELETE", url=self._delete_many_url, data=data
        )
        response = self.session.delete(
            url=self._delete_many_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        response.raise_for_status()
        return response

    def _bulk_write(self, operations: List[dict]) -> Response:
        """Bulk write many records into the collection."""

        data = json.dumps(operations)
        signed_header = self._signed_request(
            method="POST", url=self._bulk_write_url, data=data
        )
        response = self.session.post(
            url=self._bulk_write_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        return response

    def retrieve_docdb_records(
        self,
        filter_query: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
        paginate: Optional[bool] = None,
        paginate_batch_size: Optional[int] = None,
        paginate_max_iterations: Optional[int] = None,
    ) -> List[dict]:
        """
        Retrieve raw json records from DocDB API Gateway as a list of dicts.
        Queries to the API Gateway are paginated.

        Parameters
        ----------
        filter_query : Optional[dict]
          Filter to apply to the records being returned. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.
        paginate : bool
          (deprecated) If set to true, will batch the queries to the API
          Gateway.
        paginate_batch_size : int
          (deprecated) Number of records to return at a time. Default is 500.
        paginate_max_iterations : int
          (deprecated) Max number of iterations to run to prevent indefinite
          calls to the API Gateway. Default is 20000.

        Returns
        -------
        List[dict]

        """
        get_all_records = True if limit == 0 else False
        records = []
        skip = 0
        while get_all_records or limit > 0:
            batched_records = self._find_records(
                filter_query=filter_query,
                projection=projection,
                sort=sort,
                limit=limit,
                skip=skip,
            )
            batch_size = len(batched_records)
            logging.debug(
                f"(skip={skip}, limit={limit}): Retrieved {batch_size} records"
            )
            if batch_size == 0:
                break
            records.extend(batched_records)
            skip += batch_size
            if not get_all_records:
                limit -= batch_size
        return records

    def aggregate_docdb_records(self, pipeline: List[dict]) -> List[dict]:
        """Aggregate records using an aggregation pipeline."""
        return self._aggregate_records(pipeline=pipeline)

    def fetch_records_by_filter_list(
        self,
        filter_key: str,
        filter_values: List[str],
        projection: Optional[dict] = None,
    ) -> List[dict]:
        """
        Queries DocDB for records where the value of a specified field is in a
        list of values. Uses an aggregation pipeline with $in filter operator.

        Parameters
        ----------
        filter_key : str
          The field to filter on.
        filter_values : List[str]
          The list of values to filter on.
        projection : Optional[dict]
          Subset of fields to return. Default is None which returns all fields.

        Returns
        -------
        List[dict]
        """
        agg_pipeline = [{"$match": {filter_key: {"$in": filter_values}}}]
        if projection:
            agg_pipeline.append({"$project": projection})
        return self.aggregate_docdb_records(pipeline=agg_pipeline)

    def insert_one_docdb_record(self, record: dict) -> Response:
        """Insert one new record"""
        response = self._insert_one_record(
            json.loads(json.dumps(record, default=str)),
        )
        return response

    def upsert_one_docdb_record(self, record: dict) -> Response:
        """Upsert one record if the record is not corrupt"""
        if record.get("_id") is None:
            raise ValueError("Record does not have an _id field.")
        response = self._upsert_one_record(
            record_filter={"_id": record["_id"]},
            update={"$set": json.loads(json.dumps(record, default=str))},
        )
        return response

    def delete_one_record(self, data_asset_record_id: str) -> Response:
        """Delete one record by id"""

        response = self._delete_one_record(
            record_filter={"_id": data_asset_record_id},
        )
        return response

    def delete_many_records(
        self, data_asset_record_ids: List[str]
    ) -> Response:
        """Delete many records by their ids"""

        response = self._delete_many_records(
            record_filter={"_id": {"$in": data_asset_record_ids}},
        )
        return response

    @staticmethod
    def _record_to_operation(record: str, record_id: str) -> dict:
        """Maps a record into an operation"""
        return {
            "UpdateOne": {
                "filter": {"_id": record_id},
                "update": {"$set": json.loads(record)},
                "upsert": "True",
            }
        }

    def upsert_list_of_docdb_records(
        self,
        records: List[dict],
        max_payload_size: int = 5e6,
    ) -> List[Response]:
        """
        Upsert a list of records. There's a limit to the size of the
        request that can be sent, so we chunk the requests.

        Parameters
        ----------

        records : List[dict]
          List of records to upsert into the DocDB database
        max_payload_size : int
          Chunk requests into smaller lists no bigger than this value in bytes.
          If a single record is larger than this value in bytes, an attempt
          will be made to upsert the record but will most likely receive a 413
          status code. The Default is 2e6 bytes. The max payload for the API
          Gateway including headers is 10MB.

        Returns
        -------
        List[Response]
          A list of responses from the API Gateway.

        """
        if len(records) == 0:
            return []
        else:
            # check no record is corrupt or missing _id
            for record in records:
                if record.get("_id") is None:
                    raise ValueError("A record does not have an _id field.")
            # chunk records
            first_index = 0
            end_index = len(records)
            second_index = 1
            responses = []
            record_json = json.dumps(records[first_index], default=str)
            total_size = getsizeof(record_json)
            operations = [
                self._record_to_operation(
                    record=record_json,
                    record_id=records[first_index].get("_id"),
                )
            ]
            while second_index < end_index + 1:
                # TODO: Add optional progress bar?
                if second_index == end_index:
                    response = self._bulk_write(operations)
                    responses.append(response)
                else:
                    record_json = json.dumps(
                        records[second_index], default=str
                    )
                    record_size = getsizeof(record_json)
                    if total_size + record_size > max_payload_size:
                        response = self._bulk_write(operations)
                        responses.append(response)
                        first_index = second_index
                        operations = [
                            self._record_to_operation(
                                record=record_json,
                                record_id=records[first_index].get("_id"),
                            )
                        ]
                        total_size = record_size
                    else:
                        operations.append(
                            self._record_to_operation(
                                record=record_json,
                                record_id=records[second_index].get("_id"),
                            )
                        )
                        total_size += record_size
                second_index = second_index + 1
        return responses

    def close(self):
        """Close the clients."""
        if self.session is not None:
            self.session.close()

    def __enter__(self):
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        self.close()


class MetadataDbClient(Client):
    """Class to manage reading and writing to metadata db"""

    def __init__(
        self,
        host: str,
        database: str = "metadata_index",
        collection: str = "data_assets",
        version: str = "v1",
        boto: Optional[BotoSession] = None,
        session: Optional[Session] = None,
    ):
        """
        Instantiate a MetadataDbClient.

        Parameters
        ----------
        host : str
        database : str
        collection : str
        version : str
        boto : Optional[BotoSession]
        session : Optional[Session]
        """
        super().__init__(
            host=host,
            database=database,
            collection=collection,
            version=version,
            boto=boto,
            session=session,
        )

    @property
    def _data_summary_url(self) -> str:
        """Url to get LLM-generated summaries"""
        return f"https://{self.host}/{self.version}/data_summary"

    @property
    def _register_asset_url(self) -> str:
        """Url to register an asset to DocDB and Code Ocean"""
        return f"https://{self.host}/{self.version}/assets/register"

    @property
    def _deregister_asset_url(self) -> str:
        """Url to deregister (delete) an asset in DocDB and Code Ocean"""
        return f"https://{self.host}/{self.version}/assets/deregister"

    def generate_data_summary(self, record_id: str) -> Dict[str, Any]:
        """Get an LLM-generated summary for a data asset."""
        url = f"{self._data_summary_url}/{record_id}"
        signed_header = self._signed_request(method="GET", url=url)
        response = self.session.get(
            url=url, headers=dict(signed_header.headers)
        )
        response.raise_for_status()
        return response.json()

    def register_asset(self, s3_location: str) -> Dict[str, Any]:
        """Register a data asset to Code Ocean and the DocDB metadata index."""

        data = json.dumps({"s3_location": s3_location})
        signed_header = self._signed_request(
            method="POST", url=self._register_asset_url, data=data
        )
        response = self.session.post(
            url=self._register_asset_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        response.raise_for_status()
        return response.json()

    def deregister_asset(self, s3_location: str) -> Dict[str, Any]:
        """De-register (delete) a data asset in Code Ocean and the
        DocDB metadata index."""

        data = json.dumps({"s3_location": s3_location})
        signed_header = self._signed_request(
            method="DELETE", url=self._deregister_asset_url, data=data
        )
        response = self.session.delete(
            url=self._deregister_asset_url,
            headers=dict(signed_header.headers),
            data=data,
        )
        response.raise_for_status()
        return response.json()


class AnalysisDbClient(Client):
    """Class to manage reading and writing to analysis db"""

    def __init__(
        self,
        host: str,
        collection: str,
        database: str = "analysis",
        version: str = "v1",
        boto: Optional[BotoSession] = None,
        session: Optional[Session] = None,
    ):
        """
        Instantiate a AnalysisDbClient.

        Parameters
        ----------
        host : str
        collection : str
        database : str
        version : str
        boto : Optional[BotoSession]
        session : Optional[Session]
        """
        super().__init__(
            host=host,
            database=database,
            collection=collection,
            version=version,
            boto=boto,
            session=session,
        )


class SchemaDbClient(Client):
    """Class to manage reading and writing to schemas db"""

    def __init__(
        self,
        host: str,
        collection: str,
        database: str = "schemas",
        version: str = "v1",
        boto: Optional[BotoSession] = None,
        session: Optional[Session] = None,
    ):
        """Class constructor"""
        super().__init__(
            host=host,
            database=database,
            collection=collection,
            version=version,
            boto=boto,
            session=session,
        )

    def retrieve_schema_records(
        self,
        schema_version: Optional[str] = None,
        projection: Optional[dict] = None,
        sort: Optional[dict] = None,
        limit: int = 0,
    ) -> List[dict]:
        """
        Retrieve schemas records from DocDB API Gateway as a list of dicts.

        Parameters
        ----------
        schema_version : Optional[str]
          Schema version to use as a filter_query. Default is None.
        projection : Optional[dict]
          Subset of document fields to return. Default is None.
        sort : Optional[dict]
          Sort records when returned. Default is None.
        limit : int
          Return a smaller set of records. 0 for all records. Default is 0.

        Returns
        -------
        List[dict]

        """

        filter_query = (
            {"_id": schema_version} if schema_version is not None else None
        )

        records = self._get_records(
            filter_query=filter_query,
            projection=projection,
            sort=sort,
            limit=limit,
        )
        return records
