Examples - DocDB Direct Connection (SSH)
========================================

This page provides examples to interact with the Document Database (DocDB)
using the provided Python client and SSH tunneling.

It is assumed that the required credentials are set in environment.
Please refer to the User Guide for more details.

.. note::

    It is recommended to use the REST API client instead of the SSH client for ease of use.


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
      print("First 3 breeding groups and corresponding subjects:")
      print(json.dumps(result[:3], indent=3))

For more info about aggregations, please see MongoDB documentation:
https://www.mongodb.com/docs/manual/aggregation/