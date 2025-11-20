"""Test rds_tables module."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from aind_data_access_api.rds_tables import Client, RDSCredentials


class TestRDSCredentials(unittest.TestCase):
    """Tests RDSCredentials class"""

    def test_validate_database_name(self):
        """Tests that database is set from dbname as expected."""
        creds = RDSCredentials(
            username="user",
            password="password",
            host="localhost",
            port=5432,
            dbname="test_db",
        )
        self.assertEqual(creds.database, "test_db")

        creds2 = RDSCredentials(
            username="user",
            password="password",
            host="localhost",
            port=5432,
            database="test_db",
        )
        self.assertEqual(creds2.database, "test_db")
        self.assertIsNone(creds2.dbname)

        with self.assertRaises(ValueError):
            RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                port=5432,
            )


class TestClient(unittest.TestCase):
    """Tests methods in the Client class."""

    @patch("redshift_connector.connect")
    def test_read_table(self, mock_connect: MagicMock):
        """Tests that read_table returns a pandas df."""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("a", 1), ("b", 2)]

        df1 = rds_client.read_table("some_table", where_clause=None)
        df2 = rds_client.read_table(
            "some_table",
            where_clause="subject_id=0",
        )

        # Verify connections were made
        self.assertEqual(mock_connect.call_count, 2)

        # Verify queries were executed
        execute_calls = mock_cursor.execute.call_args_list
        self.assertEqual(execute_calls[0][0][0], 'SELECT * FROM "some_table"')
        self.assertEqual(
            execute_calls[1][0][0],
            'SELECT * FROM "some_table" WHERE subject_id=0',
        )

        # Verify dataframes were created
        self.assertEqual(len(df1), 2)
        self.assertEqual(len(df2), 2)

    @patch("redshift_connector.connect")
    def test_overwrite_table_with_df(self, mock_connect: MagicMock):
        """Test overwrite table method"""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        df1 = pd.DataFrame([["a", 1], ["b", 2]], columns=["foo", "bar"])

        rds_client.overwrite_table_with_df(df1, "some_table")

        # Verify cursor methods were called
        self.assertTrue(mock_cursor.execute.called)
        execute_calls = [
            call[0][0] for call in mock_cursor.execute.call_args_list
        ]
        # Should have DROP TABLE and CREATE TABLE calls
        self.assertTrue(
            any("DROP TABLE" in call for call in execute_calls)
        )
        self.assertTrue(
            any("CREATE TABLE" in call for call in execute_calls)
        )
        self.assertTrue(mock_conn.commit.called)

    @patch("redshift_connector.connect")
    def test_append_df_to_table(self, mock_connect: MagicMock):
        """Test append df to table method"""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        df1 = pd.DataFrame([["a", 1], ["b", 2]], columns=["foo", "bar"])

        rds_client.append_df_to_table(df1, "some_table")

        # Verify cursor execute was called for inserts
        self.assertEqual(mock_cursor.execute.call_count, 2)  # 2 rows
        self.assertTrue(mock_conn.commit.called)

    @patch("redshift_connector.connect")
    def test_execute_query(self, mock_connect: MagicMock):
        """Tests that a sql query gets executed."""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        res = rds_client.execute_query('SELECT * FROM "some_table"')

        # Verify query was executed
        mock_cursor.execute.assert_called_once_with(
            'SELECT * FROM "some_table"'
        )
        self.assertTrue(mock_conn.commit.called)
        self.assertEqual(res, mock_cursor)


if __name__ == "__main__":
    unittest.main()
