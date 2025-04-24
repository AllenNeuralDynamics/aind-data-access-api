Examples - DocDB REST API
==================================

This page provides examples to interact with the Document Database (DocDB)
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

  filter = {"subject.subject_id": "731015"}
  count = docdb_api_client._count_records(
      filter_query=filter,
  )
  print(count)

Filter Example 1: Get records with a certain subject_id
-------------------------------------------------------

.. code:: python

  filter = {"subject.subject_id": "731015"}
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter,
  )
  print(json.dumps(records, indent=3))


With projection (recommended):

.. code:: python

  filter = {"subject.subject_id": "731015"}
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
      "subject.breeding_info.breeding_group": "Slc17a6-IRES-Cre;Ai230-hyg(ND)"
  }
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter
  )
  print(json.dumps(records, indent=3))


With projection (recommended):

.. code:: python

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
  print("First 3 breeding groups and corresponding subjects:")
  print(json.dumps(result[:3], indent=3))

For more info about aggregations, please see MongoDB documentation:
https://www.mongodb.com/docs/manual/aggregation/

Advanced Example: Custom Session Object
-------------------------------------------

It's possible to attach a custom Session to retry certain requests errors:

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

Utility Methods
---------------

A few utility methods are provided in the :mod:`aind_data_access_api.utils` module
to help with interacting with the DocDB API.

For example, to fetch records that match any value in a list of subject IDs:

.. code:: python

    from aind_data_access_api.utils import fetch_records_by_filter_list

    records = fetch_records_by_filter_list(
        docdb_api_client=docdb_api_client,
        filter_key="subject.subject_id",
        filter_values=["731015", "741137", "789012"],
        projection={
            "name": 1,
            "location": 1,
            "subject.subject_id": 1,
            "data_description.project_name": 1,
        },
    )
    print(f"Found {len(records)} records. First 3 records:")
    print(json.dumps(records[:3], indent=3))


Updating Metadata
~~~~~~~~~~~~~~~~~~~~~~

1. **Permissions**: Request permissions for AWS Credentials to write to DocDB through the API Gateway.
2. **Query DocDB**: Filter for the records you want to update.
3. **Update DocDB**: Use ``upsert_one_docdb_record`` or ``upsert_list_of_docdb_records`` to update the records.

.. note::

    Records must be read and written as dictionaries from DocDB (not Pydantic models).

For example, to update the "instrument" and "session" metadata of a record in DocDB:

.. code:: python

  # filter for records you want to update
  records = docdb_api_client.retrieve_docdb_records(
      filter_query=filter,
      projection=projection, # recommended
  )
  print(f"Found {len(records)} records in DocDB matching filter.")

  for record in records:
      # NOTE: provide core metadata as dictionaries
      # e.g. update some field from the queried result
      instrument = record["instrument"] # dictionary
      instrument["instrument_type"] = "New Instrument Type"  
      # e.g. replace entirely from file
      with open(INSTRUMENT_FILE_PATH, "r") as f:
          instrument = json.load(f)
      # e.g. convert Pydantic model to dictionary
      session = session_model.model_dump()

      # update record in docdb
      record_update = {
          "_id": record["_id"],
          "instrument": instrument,
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

.. note::

    While DocumentDB supports fieldnames with special characters ("$" and "."), they are not recommended.
    There may be issues querying or updating these fields.

    It is recommended to avoid these special chars in dictionary keys. E.g. ``{"abc.py": "data"}`` can be
    written as ``{"filename": "abc.py", "some_file_property": "data"}`` instead.