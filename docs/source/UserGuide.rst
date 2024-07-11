User Guide
==========

Thank you for using ``aind-data-access-api``! This guide is
intended for scientists and engineers in AIND that wish to interface
with AIND Metadata.

We have two primary databases:
1. A document database (DocDB) to keep unstructured json documents. The DocDB contains AIND metadata.
2. A relational database to store structured tables.

Document Database (DocDB)
--------------------

- If using the REST API (Read-Only), you do not need credentials.
- If using any of the Direct Connection (SSH) methods, it is assumed that you have DocDB and
  SSH credentials (or have access to AWS Secrets Manager).


REST API (Read-Only)
~~~~~~~~~~~~~~~~~~~~~~
- TODO


Direct Connection (SSH) - Database UI (MongoDB Compass)
~~~~~~~~~~~~~~~~~~~~~~
- TODO


Direct Connection (SSH) - Python Client
~~~~~~~~~~~~~~~~~~~~~~

We have some convenience methods to interact with our Document Store.
You can create a client by explicitly setting credentials, or downloading from AWS Secrets Manager.

1. If using credentials from environment, please configure:

.. code:: bash

   DOC_DB_HOST=docdb-us-west-2-****.cluster-************.us-west-2.docdb.amazonaws.com
   DOC_DB_USERNAME=doc_db_username
   DOC_DB_PASSWORD=doc_db_password
   DOC_DB_SSH_HOST=ssh_host
   DOC_DB_SSH_USERNAME=ssh_username
   DOC_DB_SSH_PASSWORD=ssh_password

2. Usage:

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
