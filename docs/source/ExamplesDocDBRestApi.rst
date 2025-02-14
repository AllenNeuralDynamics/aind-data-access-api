Examples - DocDB REST API
==================================

This page provides examples for interact with the Document Database (DocDB)
REST API using the provided Python client.


Querying Metadata
~~~~~~~~~~~~~~~~~~~~~~

Count Example 1: Get # of records with a certain subject_id
-----------------------------------------------------------

.. code:: python

  import json

  from aind_data_access_api.document_db import MetadataDbClient

  API_GATEWAY_HOST = "api.allenneuraldynamics.org"
  DATABASE = "metadata_index"
  COLLECTION = "data_assets"

  docdb_api_client = MetadataDbClient(
      host=API_GATEWAY_HOST,
      database=DATABASE,
      collection=COLLECTION,
  )

  filter = {"subject.subject_id": "689418"}
  count = docdb_api_client._count_records(
      filter_query=filter,
  )
  print(count)

Filter Example 1: Get records with a certain subject_id
-------------------------------------------------------

.. code:: python

  filter = {"subject.subject_id": "689418"}
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter,
  )
  print(json.dumps(records, indent=3))


With projection (recommended):
      
.. code:: python

  filter = {"subject.subject_id": "689418"}
  projection = {
      "name": 1,
      "created": 1,
      "location": 1,
      "subject.subject_id": 1,
      "subject.date_of_birth": 1,
  }
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter,
      projection=projection,
  )
  print(json.dumps(records, indent=3))


Filter Example 2: Get records with a certain breeding group
-----------------------------------------------------------

.. code:: python

  filter = {
      "subject.breeding_info.breeding_group": "Chat-IRES-Cre_Jax006410"
  }
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter
  )
  print(json.dumps(records, indent=3))


With projection (recommended):

.. code:: python

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
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter,
      projection=projection,
  )
  print(json.dumps(records, indent=3))

Aggregation Example 1: Get all subjects per breeding group
----------------------------------------------------------

.. code:: python

  agg_pipeline = [
      {
          "$group": {
              "_id": "$subject.breeding_info.breeding_group",
              "subject_ids": {"$addToSet": "$subject.subject_id"},
              "count": {"$sum": 1},
          }
    }
  ]
  result = docdb_api_client.aggregate_docdb_records(
      pipeline=agg_pipeline
  )
  print(f"Total breeding groups: {len(result)}")
  print(f"First 3 breeding groups and corresponding subjects:")
  print(json.dumps(result[:3], indent=3))

For more info about aggregations, please see MongoDB documentation:
https://www.mongodb.com/docs/manual/aggregation/

Advanced Example: Custom Session Object
-------------------------------------------

It's possible to attach a custom Session to retry certain requests errors

.. code:: python

    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    from aind_data_access_api.document_db import MetadataDbClient

    API_GATEWAY_HOST = "api.allenneuraldynamics.org"
    DATABASE = "metadata_index"
    COLLECTION = "data_assets"

    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "DELETE"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)

    with MetadataDbClient(
        host=API_GATEWAY_HOST,
        database=DATABASE,
        collection=COLLECTION,
        session=session,
    ) as docdb_api_client:
        records = docdb_api_client.retrieve_docdb_records(limit=10)



Updating Metadata
~~~~~~~~~~~~~~~~~~~~~~

1. **Permissions**: Request permissions for AWS Credentials to write to DocDB through the API Gateway.
2. **Query DocDB**: Filter for the records you want to update.
3. **Update DocDB**: Use ``upsert_one_docdb_record`` or ``upsert_list_of_docdb_records`` to update the records.

For example, to update the "rig" and "session" metadata of a record in DocDB:

.. code:: python

  # filter for records you want to update
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter,
      projection=projection, # recommended
  )
  print(f"Found {len(records)} records in DocDB matching filter.")

  # update rig and session metadata for each record
  for record in records:
      record_update = {
          "_id": record["_id"],
          "rig": rig,
          "session": session
      }

      response = docdb_api_client.upsert_one_docdb_record(
          record=record_update
      )
      print(response.json())

You can also make updates to individual nested fields:

.. code:: python

  record_update = {
      "_id": record["_id"],
      "data_description.project_name": project_name, # nested field
  }

  response = docdb_api_client.upsert_one_docdb_record(
      record=record_update
  )
  print(response.json())

Please note that while DocumentDB supports fieldnames with special characters ("$" and "."), they are not recommended.
There may be issues querying or updating these fields.

It is recommended to avoid these special chars in dictionary keys, e.g. ``{"abc.py": "data"}`` can be written as ``{"filename": "abc.py", "some_file_property": "data"}`` instead.