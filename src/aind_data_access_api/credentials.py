from enum import Enum, auto
import os


class AutoName(Enum):
    """Autogenerate name using auto feature. Retrieved from
    https://stackoverflow.com/a/32313954"""

    def _generate_next_value_(name, start, count, last_values):
        return name


class EnvVarKeys(AutoName):
    MONGODB_USER: str = auto()
    MONGODB_PASSWORD: str = auto()
    MONGODB_HOST: str = auto()

    def __str__(self):
        return str(self.value)


class DocumentStoreCredentials:
    def __init__(
        self, user: str = None, password: str = None, host: str = None
    ) -> None:
        self.user = user
        self.password = password
        self.host = host

    def _resolve_from_env_vars(self):
        if self.user is None:
            self.user = os.getenv(str(EnvVarKeys.MONGODB_USER))
        if self.password is None:
            self.password = os.getenv(str(EnvVarKeys.MONGODB_PASSWORD))
        if self.host is None:
            self.host = os.getenv(str(EnvVarKeys.MONGODB_HOST))


# class Credentials:
