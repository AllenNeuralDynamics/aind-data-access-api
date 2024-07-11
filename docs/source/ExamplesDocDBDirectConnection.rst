Examples - DocDB Direct Connection
==========

This page provides examples for interact with the Document Database (DocDB)
using the provided Python client.

It is assumed that the required credentials are set in environment.
Please refer to the User Guide for more details.


Querying Metadata
~~~~~~~~~~~~~~~~~~~~~~

Count Example 1: Get # of records with a certain subject_id
------------------

.. code:: python

  import json

  from aind_data_access_api.document_db_ssh import (
      DocumentDbSSHClient,
      DocumentDbSSHCredentials,
  )

  credentials = DocumentDbSSHCredentials()
  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {"subject.subject_id": "689418"}
      count = doc_db_client.collection.count_documents(filter)
      print(count)

Filter Example 1: Get records with a certain subject_id
------------------

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {"subject.subject_id": "689418"}
      records = list(
          doc_db_client.collection.find(filter=filter)
      )
      print(json.dumps(records, indent=3))


With projection (recommended):
      
.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {"subject.subject_id": "689418"}
      projection = {
          "name": 1,
          "created": 1,
          "location": 1,
          "subject.subject_id": 1,
          "subject.date_of_birth": 1,
      }
      records = list(
          doc_db_client.collection.find(filter=filter, projection=projection)
      )
      print(json.dumps(records, indent=3))


Filter Example 2: Get records with a certain breeding group
------------------

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {
          "subject.breeding_info.breeding_group": "Chat-IRES-Cre_Jax006410"
      }
      records = list(
          doc_db_client.collection.find(filter=filter)
      )
      print(json.dumps(records, indent=3))


With projection (recommended):

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {
          "subject.breeding_info.breeding_group": "Chat-IRES-Cre_Jax006410"
      }
      projection = {
          "name": 1,
          "created": 1,
          "location": 1,
          "subject.subject_id": 1,
          "subject.breeding_info.breeding_group": 1,
      }
      records = list(
          doc_db_client.collection.find(filter=filter, projection=projection)
      )
      print(json.dumps(records, indent=3))

Aggregation Example 1: Get all subjects per breeding group
------------------

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      agg_pipeline = [
          {
              "$group": {
                  "_id": "$subject.breeding_info.breeding_group",
                  "subject_ids": {"$addToSet": "$subject.subject_id"},
              }
          }
      ]
      result = list(
          doc_db_client.collection.aggregate(pipeline=agg_pipeline)
      )
      print(f"Total breeding groups: {len(result)}")
      print(f"First 3 breeding groups and corresponding subjects:")
      print(json.dumps(result[:3], indent=3))


Updating Metadata
~~~~~~~~~~~~~~~~~~~~~~

We provide several utility functions for interacting with DocDB within the
``aind_data_access_api.utils`` module. Below is an example of how to use these
functions to update records in DocDB.

.. code:: python

  import json
  import logging
  from typing import List, Optional

  from aind_data_access_api.document_db_ssh import (
      DocumentDbSSHClient,
      DocumentDbSSHCredentials,
  )
  from aind_data_schema.core.metadata import Metadata

  from aind_data_access_api.utils import paginate_docdb, is_dict_corrupt

  logging.basicConfig(level="INFO")

  def _process_docdb_records(records: List[dict], doc_db_client: DocumentDbSSHClient, dryrun: bool) -> None:
      """
      Process records.
      Parameters
      ----------
      records : List[dict]

      """
      for record in records:
          _process_docdb_record(record=record, doc_db_client=doc_db_client, dryrun=dryrun)

  def _process_docdb_record(record: dict, doc_db_client: DocumentDbSSHClient, dryrun: bool) -> None:
      """
      Process record. This example updates the data_description.name field
      if it does not match the record.name field.

      Parameters
      ----------
      record : dict

      """
      _id = record.get("_id")
      name = record.get("name")
      location = record.get("location")
      if _id:
          if record.get("data_description") and record["data_description"].get("name") != name:
              # Option 1: update specific fields(s) only
              new_fields = {
                  "data_description.name": name
              }
              update_docdb_record_partial(record_id=_id, new_fields=new_fields, doc_db_client=doc_db_client, dryrun=dryrun)
              # Option 2: build new record Metadata.py and replace entire document with new record
              # new_record = build_new_docdb_record(record=record)
              # if new_record is not None:
              #     update_docdb_record_entire(record_id=_id, new_record=new_record, doc_db_client=doc_db_client, dryrun=dryrun)
          # else:
          #     logging.info(f"Record for {location} does not need to be updated.")
      else:
          logging.warning(f"Record for {location} does not have an _id field! Skipping.")


  def build_new_docdb_record(record: Optional[dict]) -> Optional[dict]:
      """Build new record from existing record. This example updates the
      data_description.name field if it does not match the record.name field.

      Parameters
      ----------
      record : Optional[dict]

      Returns
      -------
      Optional[dict]
          The new record, or None if the record cannot be constructed.
      """
      # Example: Update record.data_description.name if not matching record.name
      new_record = None
      if record.get("data_description") and record["data_description"].get("name") != name:
          _id = record.get("_id")
          name = record.get("name")
          location = record.get("location")
          created = record.get("created")
          if _id is None or name is None or location is None or created is None:
              logging.warning(f"Record does not have _id, name, location, or created! Skipping.")
              return None
          try:
              new_record = record.copy()
              new_record["data_description"]["name"] = name
              new_record_str = Metadata.model_construct(
                  **new_record
              ).model_dump_json(warnings=False, by_alias=True)
              new_record = json.loads(new_record_str)
              if is_dict_corrupt(new_record):
                  logging.warning(f"New record for {location} is corrupt! Skipping.")
                  new_record = None
          except Exception:
              new_record = None
      return new_record

  def update_docdb_record_partial(record_id: str, new_fields: dict, doc_db_client: DocumentDbSSHClient, dryrun: bool) -> None:
      """
      Update record in docdb by updating specific fields only.
      Parameters
      ----------
      record_id : str
          The _id of the record to update.
      new_fields : dict
          New fields to update. E.g. {"data_description.name": "new_name"}

      """
      if dryrun:
          logging.info(f"(dryrun) doc_db_client.collection.update_one (partial): {record_id}")
      else:
          logging.info(f"doc_db_client.collection.update_one (partial): {record_id}")
          response = doc_db_client.collection.update_one(
              {"_id": record_id},
              {"$set": new_fields},
              upsert=False,
          )
          logging.info(response.raw_result)


  def update_docdb_record_entire(record_id: str, new_record: dict, doc_db_client: DocumentDbSSHClient, dryrun: bool) -> None:
      """
      Update record in docdb by replacing the entire document with new record.
      Parameters
      ----------
      record_id : str
          The _id of the record to update.
      new_record : dict
          The new record to replace the existing record with.

      """
      if is_dict_corrupt(new_record) or record_id != new_record.get("_id"):
          logging.warning(f"Attempting to update corrupt record {record_id}! Skipping.")
          return
      if dryrun:
          logging.info(f"(dryrun) doc_db_client.collection.update_one: {record_id}")
      else:
          logging.info(f"doc_db_client.collection.update_one: {record_id}")
          response = doc_db_client.collection.update_one(
              {"_id": record_id},
              {"$set": new_record},
              upsert=False,
          )
          logging.info(response.raw_result)
            
          
  if __name__ == "__main__":
      credentials = DocumentDbSSHCredentials()    # credentials in environment
      dryrun = True
      filter = {"location": {"$regex": ".*s3://codeocean-s3datasetsbucket.*"}}
      projection = None
      
      with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
          db_name = doc_db_client.database_name
          col_name = doc_db_client.collection_name
          # count = doc_db_client.collection.count_documents(filter)
          # logging.info(f"{db_name}.{col_name}: Found {count} records with {filter}: {count}")

          logging.info(f"{db_name}.{col_name}: Starting to scan for {filter}.")
          docdb_pages = paginate_docdb(
              db_name=doc_db_client.database_name,
              collection_name=doc_db_client.collection_name,
              docdb_client=doc_db_client._client,
              page_size=500,
              filter_query=filter,
          )
          for page in docdb_pages:
              _process_docdb_records(records=page, doc_db_client=doc_db_client, dryrun=dryrun)
          logging.info(f"{db_name}.{col_name}:Finished scanning through DocDb.")