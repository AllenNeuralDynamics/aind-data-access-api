Examples - DocDB Direct Connection
==================================

This page provides examples for interact with the Document Database (DocDB)
using the provided Python client.

It is assumed that the required credentials are set in environment.
Please refer to the User Guide for more details.


Querying Metadata
~~~~~~~~~~~~~~~~~~~~~~

Count Example 1: Get # of records with a certain subject_id
-----------------------------------------------------------

.. code:: python

  import json

  from aind_data_access_api.document_db_ssh import (
      DocumentDbSSHClient,
      DocumentDbSSHCredentials,
  )

  credentials = DocumentDbSSHCredentials()
  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {"subject.subject_id": "731015"}
      count = doc_db_client.collection.count_documents(filter)
      print(count)

Filter Example 1: Get records with a certain subject_id
-------------------------------------------------------

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {"subject.subject_id": "731015"}
      records = list(
          doc_db_client.collection.find(filter=filter)
      )
      print(json.dumps(records, indent=3))


With projection (recommended):
      
.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {"subject.subject_id": "731015"}
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
-----------------------------------------------------------

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {
          "subject.breeding_info.breeding_group": "Slc17a6-IRES-Cre;Ai230-hyg(ND)"
      }
      records = list(
          doc_db_client.collection.find(filter=filter)
      )
      print(json.dumps(records, indent=3))


With projection (recommended):

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      filter = {
          "subject.breeding_info.breeding_group": "Slc17a6-IRES-Cre;Ai230-hyg(ND)"
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
----------------------------------------------------------

.. code:: python

  with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      agg_pipeline = [
          {
              "$group": {
                  "_id": "$subject.breeding_info.breeding_group",
                  "subject_ids": {"$addToSet": "$subject.subject_id"},
                  "count": {"$sum": 1},
              }
          }
      ]
      result = list(
          doc_db_client.collection.aggregate(pipeline=agg_pipeline)
      )
      print(f"Total breeding groups: {len(result)}")
      print(f"First 3 breeding groups and corresponding subjects:")
      print(json.dumps(result[:3], indent=3))

For more info about aggregations, please see MongoDB documentation:
https://www.mongodb.com/docs/manual/aggregation/


Updating Metadata
~~~~~~~~~~~~~~~~~~~~~~

Below is an example of how to update records in DocDB using ``DocumentDbSSHClient``.

.. code:: python

  import logging

  from aind_data_access_api.document_db_ssh import (
      DocumentDbSSHClient,
      DocumentDbSSHCredentials,
  )

  logging.basicConfig(level="INFO")

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
              # update specific fields(s) only
              new_fields = {
                  "data_description.name": name
              }
              update_docdb_record_partial(record_id=_id, new_fields=new_fields, doc_db_client=doc_db_client, dryrun=dryrun)
          # else:
          #     logging.info(f"Record for {location} does not need to be updated.")
      else:
          logging.warning(f"Record for {location} does not have an _id field! Skipping.")


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
            
          
  if __name__ == "__main__":
      credentials = DocumentDbSSHCredentials()    # credentials in environment
      dryrun = True
      filter = {"location": {"$regex": ".*s3://aind-open-data.*"}}
      projection = None
      
      with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
          db_name = doc_db_client.database_name
          col_name = doc_db_client.collection_name
          # count = doc_db_client.collection.count_documents(filter)
          # logging.info(f"{db_name}.{col_name}: Found {count} records with {filter}")

          logging.info(f"{db_name}.{col_name}: Starting to scan for {filter}.")
          records = doc_db_client.collection.find(
              filter=filter,
          )
          for record in records:
              _process_docdb_record(record=record, doc_db_client=doc_db_client, dryrun=dryrun)
          logging.info(f"{db_name}.{col_name}:Finished scanning through DocDb.")