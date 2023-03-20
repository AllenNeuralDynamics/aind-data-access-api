"""Test credentials module"""

import os
import unittest
from unittest.mock import patch

from aind_data_access_api.credentials import (
    DocumentStoreCredentials,
    EnvVarKeys,
)


class TestEnvVarKeys(unittest.TestCase):
    """Tests that the env var keys enums are sound"""

    def test_env_var_keys(self):
        """Test that the string representation is same as value."""

        self.assertEqual(
            EnvVarKeys.DOC_STORE_PASSWORD.value,
            str(EnvVarKeys.DOC_STORE_PASSWORD),
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_USER.value, str(EnvVarKeys.DOC_STORE_USER)
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_HOST.value, str(EnvVarKeys.DOC_STORE_HOST)
        )
        self.assertEqual(
            EnvVarKeys.DOC_STORE_PORT.value, str(EnvVarKeys.DOC_STORE_PORT)
        )


class TestDocumentStoreCredentials(unittest.TestCase):
    """Test methods in DocumentStoreCredentials class."""

    @patch.dict(
        os.environ,
        {
            "DOC_STORE_USER": "fake_user",
            "DOC_STORE_PASSWORD": "fake_password",
            "DOC_STORE_host": "localhost",
            "DOC_STORE_PORT": "12345",
        },
        clear=True,
    )
    def test_credentials_loaded_from_env(self):
        """Tests that the credentials are loaded from env vars correctly"""
        creds = DocumentStoreCredentials()
        self.assertEqual("fake_user", creds.user)
        self.assertEqual("localhost", creds.host)
        self.assertEqual(12345, creds.port)
        self.assertEqual("fake_password", creds.password.get_secret_value())


if __name__ == "__main__":
    unittest.main()
