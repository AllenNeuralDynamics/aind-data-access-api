import json
import logging

from aind_data_access_api.document_db_ssh import (
    DocumentDbSSHClient,
    DocumentDbSSHCredentials,
)

logging.basicConfig(level="INFO")


def log_example_result(filter, projection, count, records):
    logging.info(f"Total records with {filter}: {count}")
    if records:
        logging.info(
            f"First record with {filter} and projection {projection}:"
        )
        logging.info(json.dumps(records[0], indent=3))


if __name__ == "__main__":
    # set credentials in environment following .env.template
    credentials = DocumentDbSSHCredentials()

    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        # Filter example 1: Get records with a certain subject_id
        filter = {"subject.subject_id": "689418"}
        projection = {
            "name": 1,
            "created": 1,
            "location": 1,
            "subject.subject_id": 1,
            "subject.date_of_birth": 1,
        }
        count = doc_db_client.collection.count_documents(filter)
        records = list(
            doc_db_client.collection.find(filter=filter, projection=projection)
        )
        log_example_result(filter, projection, count, records)

        # Filter example 2: Get records with a certain breeding group
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
        count = doc_db_client.collection.count_documents(filter)
        records = list(
            doc_db_client.collection.find(filter=filter, projection=projection)
        )
        log_example_result(filter, projection, count, records)

        # Aggregation example 1: Get all subjects per breeding group
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
        logging.info(f"Total breeding groups: {len(result)}")
        logging.info(f"First 3 breeding groups and corresponding subjects:")
        logging.info(json.dumps(result[:3], indent=3))
