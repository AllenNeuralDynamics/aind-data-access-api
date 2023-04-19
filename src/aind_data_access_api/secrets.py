"""Module to access secrets and parameters"""
import json

import boto3


def get_secret(secret_name: str) -> dict:
    """
    Retrieves a secret from AWS Secrets Manager.

    param secret_name: The name of the secret to retrieve.
    """
    # Create a Secrets Manager client
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_name)
    finally:
        client.close()
    return json.loads(response["SecretString"])


def get_parameter(parameter_name: str, with_decryption=False) -> str:
    """
    Retrieves a parameter from AWS Parameter Store.

    param parameter_name: The name of the parameter to retrieve.
    """
    # Create a Systems Manager client
    client = boto3.client("ssm")
    try:
        response = client.get_parameter(
            Name=parameter_name, WithDecryption=with_decryption
        )
    finally:
        client.close()
    return response["Parameter"]["Value"]
