"""Test rds_tables module."""

import unittest
from unittest.mock import MagicMock, call, patch

import pandas as pd
from sqlalchemy import text

from aind_data_access_api.rds_tables import Client, RDSCredentials


class TestClient(unittest.TestCase):
    """Tests methods in the Client class."""

    @patch("pandas.read_sql_query")
    @patch("sqlalchemy.engine.Engine.begin")
    def test_read_table(self, mock_engine: MagicMock, mock_pd_read: MagicMock):
        """Tests that read_table returns a pandas df."""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        mock_pd_read.return_value = pd.DataFrame()

        df1 = rds_client.read_table("some_table", where_clause=None)
        df2 = rds_client.read_table(
            "some_table",
            where_clause="subject_id=0",
        )
        query = text('SELECT * FROM "some_table"')
        query2 = text('SELECT * FROM "some_table" WHERE subject_id=0')
        mock_engine.assert_has_calls(
            [
                call(),
                call().__enter__(),
                call().__exit__(None, None, None),
                call(),
                call().__enter__(),
                call().__exit__(None, None, None),
            ]
        )
        self.assertEqual(
            mock_pd_read.mock_calls[0].kwargs["sql"].text, query.text
        )
        self.assertEqual(
            mock_pd_read.mock_calls[1].kwargs["sql"].text, query2.text
        )
        self.assertTrue(df1.empty)
        self.assertTrue(df2.empty)

    @patch("pandas.DataFrame.to_sql")
    @patch("aind_data_access_api.rds_tables.Client._engine")
    def test_overwrite_table_with_df(
        self, mock_engine: MagicMock, mock_to_sql: MagicMock
    ):
        """Test overwrite table method"""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        df1 = pd.DataFrame([["a", 1], ["b", 2]], columns=["foo", "bar"])
        mock_engine.return_value = MagicMock()

        rds_client.overwrite_table_with_df(df1, "some_table")
        mock_to_sql.assert_called_once_with(
            name="some_table",
            con=rds_client._engine,
            dtype=None,
            method="multi",
            if_exists="replace",
            index=False,
        )

    @patch("pandas.DataFrame.to_sql")
    @patch("aind_data_access_api.rds_tables.Client._engine")
    def test_append_df_to_table(
        self, mock_engine: MagicMock, mock_to_sql: MagicMock
    ):
        """Test append df to table method"""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )

        df1 = pd.DataFrame([["a", 1], ["b", 2]], columns=["foo", "bar"])
        mock_engine.return_value = MagicMock()

        rds_client.append_df_to_table(df1, "some_table")
        mock_to_sql.assert_called_once_with(
            name="some_table",
            con=rds_client._engine,
            dtype=None,
            method="multi",
            if_exists="append",
            index=False,
        )

    @patch("sqlalchemy.engine.Engine.begin")
    def test_execute_query(self, mock_engine: MagicMock):
        """Tests that a sql query gets executed."""
        rds_client = Client(
            credentials=RDSCredentials(
                username="user",
                password="password",
                host="localhost",
                database="db",
            ),
        )
        mock_exec = mock_engine.return_value.__enter__.return_value.execute
        mock_exec.return_value = "some result"
        res = rds_client.execute_query('SELECT * FROM "some_table"')
        self.assertEqual("some result", res)
        input_text = mock_exec.mock_calls[0].args[0].text
        self.assertEqual('SELECT * FROM "some_table"', input_text)


if __name__ == "__main__":
    unittest.main()
