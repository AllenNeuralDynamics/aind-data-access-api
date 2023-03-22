# aind-data-access-api

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)

API to interact with a few AIND databases.

## Usage
We have two primary databases. A Document store to keep unstructured json documents, and a relational database to store structured tables.

### Document Store
We have some convenience methods to interact with our Document Store. You can create a client by explicitly setting credentials, or downloading from AWS Secrets Manager.
```
from aind_data_access_api.credentials import DocumentStoreCredentials
from aind_data_access_api.document_store import Client

# Method one assuming user, password, and host are known
ds_client = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="host",
                database="metadata",
            ),
            collection_name="data_assets",
        )

# Method two if you have permissions to AWS Secrets Manager
ds_client = Client(
            credentials=DocumentStoreCredentials(
                aws_secrets_name="aind/data/access/api/document_store/metadata"
            ),
            collection_name="data_assets",
        )

# To get all records
response = list(ds_client.retrieve_data_asset_records())

# To get a list of filtered records:
response = list(ds_client.retrieve_data_asset_records({"subject.subject_id": "123456"}))
```

### RDS Tables
We have some convenience methods to interact with our Relational Database. You can create a client by explicitly setting credentials, or downloading from AWS Secrets Manager.
```
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
```

## Installation
To use the software, it can be installed from PyPI.
```bash
pip install aind-data-access-api
```

To develop the code, clone repo and run
```bash
pip install -e .[dev]
```

## Contributing

### Linters and testing

There are several libraries used to run linters, check documentation, and run tests.

- Please test your changes using the **coverage** library, which will run the tests and log a coverage report:

```bash
coverage run -m unittest discover && coverage report
```

- Use **interrogate** to check that modules, methods, etc. have been documented thoroughly:

```bash
interrogate .
```

- Use **flake8** to check that code is up to standards (no unused imports, etc.):
```bash
flake8 .
```

- Use **black** to automatically format the code into PEP standards:
```bash
black .
```

- Use **isort** to automatically sort import statements:
```bash
isort .
```

### Pull requests

For internal members, please create a branch. For external members, please fork the repository and open a pull request from the fork. We'll primarily use [Angular](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit) style for commit messages. Roughly, they should follow the pattern:
```text
<type>(<scope>): <short summary>
```

where scope (optional) describes the packages affected by the code changes and type (mandatory) is one of:

- **build**: Changes that affect build tools or external dependencies (example scopes: pyproject.toml, setup.py)
- **ci**: Changes to our CI configuration files and scripts (examples: .github/workflows/ci.yml)
- **docs**: Documentation only changes
- **feat**: A new feature
- **fix**: A bugfix
- **perf**: A code change that improves performance
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **test**: Adding missing tests or correcting existing tests

### Semantic Release

The table below, from [semantic release](https://github.com/semantic-release/semantic-release), shows which commit message gets you which release type when `semantic-release` runs (using the default configuration):

| Commit message                                                                                                                                                                                   | Release type                                                                                                    |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `fix(pencil): stop graphite breaking when too much pressure applied`                                                                                                                             | ~~Patch~~ Fix Release, Default release                                                                          |
| `feat(pencil): add 'graphiteWidth' option`                                                                                                                                                       | ~~Minor~~ Feature Release                                                                                       |
| `perf(pencil): remove graphiteWidth option`<br><br>`BREAKING CHANGE: The graphiteWidth option has been removed.`<br>`The default graphite width of 10mm is always used for performance reasons.` | ~~Major~~ Breaking Release <br /> (Note that the `BREAKING CHANGE: ` token must be in the footer of the commit) |

### Documentation
To generate the rst files source files for documentation, run
```bash
sphinx-apidoc -o doc_template/source/ src 
```
Then to create the documentation HTML files, run
```bash
sphinx-build -b html doc_template/source/ doc_template/build/html
```
More info on sphinx installation can be found [here](https://www.sphinx-doc.org/en/master/usage/installation.html).
