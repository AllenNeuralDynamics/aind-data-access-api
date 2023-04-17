"""Module to access secrets and parameters"""
import boto3
import json
from botocore.exceptions import ClientError
from enum import Enum


class ClientExceptions(Enum):
    """Enum class of Exceptions"""
    RESOURCE_NOT_FOUND = "ResourceNotFoundException"
    INVALID_REQUEST = "InvalidRequestException"
    INVALID_PARAMETER = "InvalidParameterException"
    PARAMETER_NOT_FOUND = "ParameterNotFound"
    INVALID_KEY_ID = "InvalidKeyId"
    ACCESS_DENIED = "AccessDeniedException"


def get_secret(secret_name: str):
    """Retrieves specified secret if user has proper aws permissions"""
    # Create a secrets manager client
    client = boto3.client('secretsmanager')

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == ClientExceptions.RESOURCE_NOT_FOUND.value:
            print(f"The secret with name {secret_name} was not found.")
        elif e.response['Error']['Code'] == ClientExceptions.INVALID_REQUEST.value:
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == ClientExceptions.INVALID_PARAMETER.value:
            print("The request had invalid parameters:", e)
        else:
            print("Unknown error occurred:", e)
        return None
    else:
        # Return the secret value
        return json.loads(response['SecretString'])['password']


def get_parameter(parameter_name: str):
    """Retrieves specified parameter if user has proper aws permissions"""
    # Create a Systems Manager client
    client = boto3.client('ssm')

    try:
        response = client.get_parameter(Name=parameter_name)
    except ClientError as e:
        if e.response['Error']['Code'] == ClientExceptions.PARAMETER_NOT_FOUND.value:
            print(f"The parameter with name {parameter_name} was not found.")
        elif e.response['Error']['Code'] == ClientExceptions.INVALID_KEY_ID.value:
            print("The AWS access key ID provided does not exist in our records.")
        elif e.response['Error']['Code'] == ClientExceptions.ACCESS_DENIED.value:
            print("User does not have permission to access the parameter.")
        else:
            print("Unknown error occurred:", e)
        return None
    else:
        return response['Parameter']['Value']
