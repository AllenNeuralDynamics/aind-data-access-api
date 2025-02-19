"""Module to manage credentials to connect to databases."""

import functools
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, Type

import boto3
from pydantic import Field, SecretStr
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    InitSettingsSource,
    PydanticBaseSettingsSource,
)


class JsonConfigSettingsSource(PydanticBaseSettingsSource, ABC):
    """Abstract base class for settings that parse json"""

    def __init__(self, settings_cls, config_file_location):
        """
        Class constructor for generic settings source that parses json
        Parameters
        ----------
        settings_cls
          Required for parent init
        config_file_location
          Location of json contents to parse
        """
        self.config_file_location = config_file_location
        super().__init__(settings_cls)

    @abstractmethod
    def _retrieve_contents(self) -> Dict[str, Any]:
        """Retrieve contents from config_file_location"""

    @functools.cached_property
    def _json_contents(self):
        """Cache contents to a property to avoid re-downloading."""
        contents = self._retrieve_contents()
        return contents

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        """
        Gets the value, the key for model creation, and a flag to determine
        whether value is complex.
        Parameters
        ----------
        field : FieldInfo
          The field
        field_name : str
          The field name

        Returns
        -------
        Tuple[Any, str, bool]
          A tuple contains the key, value and a flag to determine whether
          value is complex.

        """
        file_content_json = self._json_contents
        field_value = file_content_json.get(field_name)
        return field_value, field_name, False

    def prepare_field_value(
        self,
        field_name: str,
        field: FieldInfo,
        value: Any,
        value_is_complex: bool,
    ) -> Any:
        """
        Prepares the value of a field.
        Parameters
        ----------
        field_name : str
          The field name
        field : FieldInfo
          The field
        value : Any
          The value of the field that has to be prepared
        value_is_complex : bool
          A flag to determine whether value is complex

        Returns
        -------
        Any
          The prepared value

        """
        return value

    def __call__(self) -> Dict[str, Any]:
        """
        Run this when this class is called. Required to be implemented.

        Returns
        -------
        Dict[str, Any]
          The fields for the settings defined as a dict object.

        """
        d: Dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value

        return d


class AWSConfigSettingsSource(JsonConfigSettingsSource):
    """Class that parses from aws secrets manager."""

    @staticmethod
    def _get_secret(secret_name: str) -> Dict[str, Any]:
        """
        Retrieves a secret from AWS Secrets Manager.

        Parameters
        ----------
        secret_name : str
          Secret name as stored in Secrets Manager

        Returns
        -------
        Dict[str, Any]
          Contents of the secret

        """
        # Create a Secrets Manager client
        client = boto3.client("secretsmanager")
        try:
            response = client.get_secret_value(SecretId=secret_name)
        finally:
            client.close()
        return json.loads(response["SecretString"])

    def _retrieve_contents(self) -> Dict[str, Any]:
        """Retrieve contents from config_file_location"""
        credentials_from_aws = self._get_secret(self.config_file_location)
        return credentials_from_aws


class CoreCredentials(BaseSettings):
    """Core credentials for most of our databases."""

    # Optional field the user can set to pull secrets from AWS Secrets Manager
    # Will not be printed in repr string.
    aws_secrets_name: Optional[str] = Field(default=None, repr=False)

    username: str = Field(...)
    password: SecretStr = Field(...)
    host: str = Field(...)
    port: int = Field(...)
    database: Optional[str] = Field(default=None)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: InitSettingsSource,
        env_settings: EnvSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Method to pull configs from a variety sources, such as a file or aws.
        Arguments are required and set by pydantic.

        Parameters
        ----------
        settings_cls : Type[BaseSettings]
          Top level class. Model fields can be pulled from this.
        init_settings : InitSettingsSource
          The settings in the init arguments.
        env_settings : EnvSettingsSource
          The settings pulled from environment variables.
        dotenv_settings : PydanticBaseSettingsSource
          Settings from .env files. Currently, not supported.
        file_secret_settings : PydanticBaseSettingsSource
          Settings from secret files such as used in Docker. Currently, not
          supported.

        Returns
        -------
        Tuple[PydanticBaseSettingsSource, ...]

        """
        aws_secrets_path = init_settings.init_kwargs.get("aws_secrets_name")

        # If user defines aws secrets, create creds from there
        if aws_secrets_path is not None:
            return (
                init_settings,
                AWSConfigSettingsSource(settings_cls, aws_secrets_path),
            )
        # Otherwise, create creds from init and env
        else:
            return (
                init_settings,
                env_settings,
            )
