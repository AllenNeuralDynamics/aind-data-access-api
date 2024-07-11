User Guide
==========

Thank you for using ``aind-data-access-api``! This guide is
intended for scientists and engineers in AIND that wish to interface
with AIND databases.

We have two primary databases:

1. A `document database (DocDB) <#document-database-docdb>`__ to store
   unstructured json documents. The DocDB contains AIND metadata.
2. A `relational database <#rds-tables>`__ to store structured tables.

Document Database (DocDB)
--------------------

AIND metadata records stored in the DocDB describe the ``metadata.nd.json``
for a data asset:

- ``_id``: the unique ID of the data asset.
- ``name``: the name of the data asset.
- ``location``: the S3 location of the metadata, in the format
  ``s3://{bucket_name}/{name}``. This is unique across records and can
  be used to query or identify specific records.
- Please see the `readthedocs for aind-data-schema 
  <https://aind-data-schema.readthedocs.io/en/latest/aind_data_schema.core.html#module-aind_data_schema.core.metadata>`__
  for more details.

The DocDB can be accessed through a public read-only REST API or
through a direct connection using SSH. For a direct connection,
it is assumed you have the appropriate credentials.

REST API (Read-Only)
~~~~~~~~~~~~~~~~~~~~~~

1. A GET request to ``https://api.allenneuraldynamics.org/v1/metadata_index/data_assets``
   with appropriate query parameters will return a list of records found.

.. code:: python

   import json
   import requests

   URL = "https://api.allenneuraldynamics.org/v1/metadata_index/data_assets"
   filter = {"subject.subject_id": "123456"}
   limit = 100
   response = requests.get(URL, params={"filter": json.dumps(filter), "limit": limit})
   print(response.json())

2. Alternatively, we provide a Python client:

.. code:: python

   from aind_data_access_api.document_db import MetadataDbClient

   API_GATEWAY_HOST = "api.allenneuraldynamics.org"
   DATABASE = "metadata_index"
   COLLECTION = "data_assets"

   docdb_api_client = MetadataDbClient(
      host=API_GATEWAY_HOST,
      database=DATABASE,
      collection=COLLECTION,
   )

   filter = {"subject.subject_id": "123456"}
   limit = 1000
   paginate_batch_size = 100
   response = docdb_api_client.retrieve_data_asset_records(
      filter_query=filter,
      limit=limit,
      paginate_batch_size=paginate_batch_size
   )
   print(response)


Direct Connection (SSH) - Database UI (MongoDB Compass)
~~~~~~~~~~~~~~~~~~~~~~

MongoDB Compass is a database GUI that can be used to query and interact
with our document database.

To connect:

1. If provided a temporary SSH password, please first run ``ssh {ssh-username}@54.184.81.236``
   and set a new password.
2. Download the full version of `MongoDB Compass <https://www.mongodb.com/try/download/compass>`__.
3. When connecting, click “Advanced Connection Options” and use the configurations below.
   Leave any unspecified fields on their default setting.

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Tab
     - Config
     - Value
   * - General
     - Host
     - ``************.us-west-2.docdb.amazonaws.com``
   * - Authentication
     - Username
     - ``doc_db_username``
   * - 
     - Password
     - ``doc_db_password``
   * - 
     - Authentication Mechanism
     - SCRAM-SHA-1
   * - TLS/SSL
     - SSL/TLS Connection
     - OFF
   * - Proxy/SSH
     - SSH Tunnel/ Proxy Method	
     - SSH with Password
   * -
     - SSH Hostname
     - ``ssh_host``
   * -
     - SSH Port
     - 22
   * -
     - SSH Username
     - ``ssh_username``
   * -
     - SSH Username
     - ``ssh_password``
   
4. You should be able to see the home page with the ``metadata-index`` database.
   It should have 1 single collection called ``data_assets``.
5. If provided with a temporary DocDB password, please change it using the embedded
   mongo shell in Compass, and then reconnect.

.. code:: bash
   
   db.updateUser(
      "doc_db_username",
      {
         pwd: passwordPrompt()
      }
   )

Direct Connection (SSH) - Python Client
~~~~~~~~~~~~~~~~~~~~~~

We have some convenience methods to interact with our Document Store.
You can create a client by explicitly setting credentials, or downloading from AWS Secrets Manager.

If using credentials from environment, please configure:

.. code:: bash

   DOC_DB_HOST=************.us-west-2.docdb.amazonaws.com
   DOC_DB_USERNAME=doc_db_username
   DOC_DB_PASSWORD=doc_db_password
   DOC_DB_SSH_HOST=ssh_host
   DOC_DB_SSH_USERNAME=ssh_username
   DOC_DB_SSH_PASSWORD=ssh_password

To use the client:

.. code:: python

   from aind_data_access_api.document_db_ssh import DocumentDbSSHClient, DocumentDbSSHCredentials

   # Method 1) if credentials are set in environment
   credentials = DocumentDbSSHCredentials()

   # Method 2) if you have permissions to AWS Secrets Manager
   # Each secret must contain corresponding "host", "username", and "password"
   credentials = DocumentDbSSHCredentials.from_secrets_manager(
      doc_db_secret_name="/doc/db/secret/name", ssh_secret_name="/ssh/tunnel/secret/name"
   )

   with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
      # To get a list of filtered records:
      filter = {"subject.subject_id": "123456"}
      projection = {
         "name": 1, "created": 1, "location": 1, "subject.subject_id": 1, "subject.date_of_birth": 1,
      }
      count = doc_db_client.collection.count_documents(filter)
      response = list(doc_db_client.collection.find(filter=filter, projection=projection))


RDS Tables
------------------
We have some convenience methods to interact with our Relational Database. You can create a client by 
explicitly setting credentials, or downloading from AWS Secrets Manager.

.. code:: python

   from aind_data_access_api.credentials import RDSCredentials
   from aind_data_access_api.rds_tables import Client

   # Method one assuming user, password, and host are known
   ds_client = Client(
               credentials=RDSCredentials(
                  username="user",
                  password="password",
                  host="host",
                  database="metadata",
               ),
               collection_name="data_assets",
         )

   # Method two if you have permissions to AWS Secrets Manager
   ds_client = Client(
               credentials=RDSCredentials(
                  aws_secrets_name="aind/data/access/api/rds_tables"
               ),
         )

   # To retrieve a table as a pandas dataframe
   df = ds_client.read_table(table_name="spike_sorting_urls")

   # Can also pass in a custom sql query
   df = ds_client.read_table(query="SELECT * FROM spike_sorting_urls")

   # It's also possible to save a pandas dataframe as a table. Please check internal documentation for more details.
   ds_client.overwrite_table_with_df(df, table_name)

Reporting bugs or making feature requests
-----------------------------------------

Please report any bugs or feature requests here:
`issues <https://github.com/AllenNeuralDynamics/aind-data-access-api/issues/new/choose>`__
