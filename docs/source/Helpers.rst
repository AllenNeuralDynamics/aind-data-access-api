Helpers
==========

For your convenience, `aind-data-access-api` contains a few helper functions that simplify interaction with some parts of `aind-data-schema`.

Installation
-------------------------

To install the helpers, run `pip install aind-data-access-api[helpers]`

Quality Control Helpers
-------------------------

To get a `QualityControl` object for a specific ID or name, you can use:

.. code:: python

   from aind_data_access_api.document_db import MetadataDbClient
   from aind_data_access_api.helpers.data_schema import get_quality_control_by_id

   API_GATEWAY_HOST = "api.allenneuraldynamics.org"
   DATABASE = "metadata_index"
   COLLECTION = "data_assets"

   client = MetadataDbClient(
      host=API_GATEWAY_HOST,
      database=DATABASE,
      collection=COLLECTION,
   )

   qc = get_quality_control_by_name(client, ["behavior_711042_2024-08-07_12-20-41"])

If you want to recover just the QC value or status, you can use:

.. code:: python

   df = get_quality_control_value_df(client, ["behavior_711042_2024-08-07_12-20-41"])