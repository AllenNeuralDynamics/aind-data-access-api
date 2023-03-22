"""Module to manage credentials to connect to databases."""

import json
import logging
from enum import Enum, auto
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseSettings, Field, SecretStr


class AutoName(Enum):
    """Autogenerate name using auto feature. Retrieved from
    https://stackoverflow.com/a/32313954"""

    def _generate_next_value_(self, start, count, last_values):
        """Hack to get auto to reflect var name instead of int"""
        return self


# TODO: There's a bug with pycharm warning about auto. The latest version
#  of Pycharm should fix it.
# noinspection PyArgumentList
class EnvVarKeys(str, AutoName):
    """Enum of the names of the environment variables to check."""

    DOC_STORE_USER = auto()
    DOC_STORE_PASSWORD = auto()
    DOC_STORE_HOST = auto()
    DOC_STORE_PORT = auto()
    DOC_STORE_DATABASE = auto()
    RDS_USER = auto()
    RDS_PASSWORD = auto()
    RDS_HOST = auto()
    RDS_PORT = auto()
    RDS_DATABASE = auto()

    def __str__(self):
        """As a default, have the string representation just be the value."""
        return str(self.value)


class CoreCredentials(BaseSettings):
    """Core credentials for most of our databases."""

    # Optional field the user can set to pull secrets from AWS Secrets Manager
    # Will not be printed in repr string.
    aws_secrets_name: Optional[str] = Field(default=None, repr=False)

    username: str = Field(...)
    password: SecretStr = Field(...)
    host: str = Field(...)
    port: int = Field(...)
    database: str = Field(...)

    class Config:
        """This class will add custom sourcing from aws."""

        @staticmethod
        def settings_from_aws(secrets_name: Optional[str]):  # noqa: C901
            """
            Curried function that returns a function to retrieve creds from aws
            Parameters
            ----------
            secrets_name : Name of the credentials we wish to retrieve

            Returns
            -------
            A function that retrieves the credentials.

            """

            def set_settings(_: BaseSettings) -> Dict[str, Any]:
                """
                A simple settings source that loads from aws secrets manager
                """
                sm_client = boto3.client("secretsmanager")
                try:
                    secret_from_aws = sm_client.get_secret_value(
                        SecretId=secrets_name
                    )
                    secret_as_string = secret_from_aws["SecretString"]
                    secrets_json = json.loads(secret_as_string)
                    secrets = {}
                    # Map to our secrets model
                    if secrets_json.get("username"):
                        secrets["username"] = secrets_json.get("username")
                    if secrets_json.get("password"):
                        secrets["password"] = secrets_json.get("password")
                    if secrets_json.get("host"):
                        secrets["host"] = secrets_json.get("host")
                    if secrets_json.get("port"):
                        secrets["port"] = secrets_json.get("port")
                    if secrets_json.get("dbname"):
                        secrets["database"] = secrets_json.get("dbname")
                    if secrets_json.get("database"):
                        secrets["database"] = secrets_json.get("database")
                except ClientError as e:
                    logging.warning(
                        f"Unable to retrieve parameters from aws: {e.response}"
                    )
                    secrets = {}
                finally:
                    sm_client.close()
                return secrets

            return set_settings

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            """Class method to return custom sources."""
            aws_secrets_name = init_settings.init_kwargs.get(
                "aws_secrets_name"
            )
            if aws_secrets_name:
                return (
                    init_settings,
                    env_settings,
                    file_secret_settings,
                    cls.settings_from_aws(secrets_name=aws_secrets_name),
                )
            else:
                return (
                    init_settings,
                    env_settings,
                    file_secret_settings,
                )
