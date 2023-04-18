"""Module to access secrets and parameters"""
import boto3
import json


def get_secret(secret_name: str):
    """
    Retrieves a secret from AWS Secrets Manager.

    param secret_name: The name of the secret to retrieve.
    return: The value of the secret (str).
    """
    # Create a Secrets Manager client
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    client.close()
    return json.loads(response["SecretString"])["password"]


def get_parameter(parameter_name: str):
    """
    Retrieves a secret from AWS Secrets Manager.

    param parameter_name: The name of the parameter to retrieve.
    return: The value of the parameter (str).
    """
    # Create a Systems Manager client
    client = boto3.client("ssm")
    response = client.get_parameter(Name=parameter_name)
    client.close()
    return response["Parameter"]["Value"]
