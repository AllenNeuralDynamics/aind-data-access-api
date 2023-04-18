"""Module to access secrets and parameters"""
import boto3
import json
from botocore.exceptions import ClientError


def get_secret(secret_name: str):
    """Retrieves specified secret if user has proper aws permissions"""
    # Create a Secrets Manager client
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError:
        return None
    finally:
        client.close()
    return json.loads(response["SecretString"])["password"]


def get_parameter(parameter_name: str):
    """Retrieves specified parameter if user has proper aws permissions"""
    # Create a Systems Manager client
    client = boto3.client("ssm")

    try:
        response = client.get_parameter(Name=parameter_name)
    except ClientError:
        return None
    finally:
        client.close()
    return response["Parameter"]["Value"]
