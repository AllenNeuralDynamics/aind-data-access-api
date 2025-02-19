"""Test document_store module."""

import unittest


from aind_data_access_api.document_store import (
    Client,
    DocumentStoreCredentials,
)


class TestClient(unittest.TestCase):
    """Test methods in Client class."""

    def test_retry_writes(self):
        """Tests that the retryWrites option can be set."""
        ds_client1 = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
            collection_name="coll",
        )
        ds_client2 = Client(
            credentials=DocumentStoreCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
            collection_name="coll",
            retry_writes=False,
        )

        self.assertTrue(
            ds_client1._client._MongoClient__options._options["retryWrites"]
        )
        self.assertFalse(
            ds_client2._client._MongoClient__options._options["retryWrites"]
        )


if __name__ == "__main__":
    unittest.main()
