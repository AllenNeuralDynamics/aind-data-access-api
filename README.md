# aind-data-access-api

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)

API to interact with a few AIND databases. We have two primary databases:

1. A document database (DocDB) to store unstructured JSON documents. The DocDB contains AIND metadata.
2. A relational database to store structured tables.

## Installation

Basic installation:
```bash
pip install aind-data-access-api
```

The package includes optional features that require additional dependencies:

### Document Database (DocDB)
To use the `MetadataDbClient` and other DocDB features:
```bash
pip install "aind-data-access-api[docdb]"
```
Note: The quotes are required when using zsh or other shells that interpret square brackets.

### Relational Database
For RDS functionality:
```bash
pip install "aind-data-access-api[rds]"
```

### Other Install Options
- AWS Secrets management: `pip install "aind-data-access-api[secrets]"`
- Helpers: `pip install "aind-data-access-api[helpers]"`
- All features: `pip install "aind-data-access-api[full]"`

More information can be found at [readthedocs](https://aind-data-access-api.readthedocs.io).