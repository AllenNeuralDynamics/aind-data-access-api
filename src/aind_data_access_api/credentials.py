"""Module to manage credentials to connect to databases."""

from enum import Enum, auto

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

    def __str__(self):
        """As a default, have the string representation just be the value."""
        return str(self.value)


class DocumentStoreCredentials(BaseSettings):
    """Credentials for the Document Store."""

    user: str = Field(..., env=EnvVarKeys.DOC_STORE_USER.value)
    password: SecretStr = Field(..., env=EnvVarKeys.DOC_STORE_PASSWORD.value)
    host: str = Field(..., env=EnvVarKeys.DOC_STORE_HOST.value)
    port: int = Field(..., env=EnvVarKeys.DOC_STORE_PORT.value)
