"""Module to manage credentials to connect to databases."""

from typing import Optional, Tuple, Type

from aind_codeocean_api.credentials import AWSConfigSettingsSource
from pydantic import Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    InitSettingsSource,
    PydanticBaseSettingsSource,
)


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
