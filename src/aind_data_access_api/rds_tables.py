"""Module to interface with the Relational Database"""

from typing import Optional, Union

import pandas as pd
import sqlalchemy.engine
from pydantic import Field, SecretStr
from sqlalchemy import create_engine, engine, text
from sqlalchemy.engine.cursor import CursorResult

from aind_data_access_api.credentials import CoreCredentials, EnvVarKeys


class RDSCredentials(CoreCredentials):
    """RDS db credentials"""

    username: str = Field(..., env=EnvVarKeys.RDS_USER.value)
    password: SecretStr = Field(..., env=EnvVarKeys.RDS_PASSWORD.value)
    host: str = Field(..., env=EnvVarKeys.RDS_HOST.value)
    port: int = Field(default=5432, env=EnvVarKeys.RDS_PORT.value)
    database: str = Field(..., env=EnvVarKeys.RDS_DATABASE.value)


class Client:
    """Class to establish a relational database client. Includes methods to
    read/write pandas dataframes to backend."""

    def __init__(self, credentials: RDSCredentials):
        """
        Construct a client to interface with relational database.
        Parameters
        ----------
        credentials : CoreCredentials
        """
        self.credentials = credentials

    @property
    def _engine(self) -> sqlalchemy.engine.Engine:
        """Create a sqlalchemy engine:
        https://docs.sqlalchemy.org/en/20/core/engines.html

        Returns: sqlalchemy.engine.Engine
        """

        connection_url = engine.URL.create(
            drivername="postgresql",
            username=self.credentials.username,
            password=self.credentials.password.get_secret_value(),
            host=self.credentials.host,
            database=self.credentials.database,
            port=self.credentials.port,
        )
        return create_engine(connection_url)

    def append_df_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        dtype: Optional[Union[dict, str]] = None,
    ) -> None:
        """
        Append a dataframe to an existing table.
        Parameters
        ----------
        df : pd.Dataframe
        table_name : str
        dtype: Optional[Union[dict, str]]

        Returns
        -------
        None

        """
        # to_sql method has types str | None, but also allows for callable
        # Suppressing type check warning.
        # noinspection PyTypeChecker
        df.to_sql(
            name=table_name,
            con=self._engine,
            dtype=dtype,
            method="multi",
            if_exists="append",
            index=False,  # Redshift doesn't support index=True
        )
        return None

    def overwrite_table_with_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        dtype: Optional[Union[dict, str]] = None,
    ) -> None:
        """
        Overwrite an existing table with a dataframe.
        Parameters
        ----------
        df : pd.Dataframe
        table_name : str
        dtype: Optional[Union[dict, str]]

        Returns
        -------
        None
        """
        # to_sql method has types str | None, but also allows for callable
        # Suppressing type check warning.
        # noinspection PyTypeChecker
        df.to_sql(
            name=table_name,
            con=self._engine,
            dtype=dtype,
            method="multi",
            if_exists="replace",
            index=False,  # Redshift doesn't support index=True
        )
        return None

    def read_table(
        self, table_name: str, where_clause: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Import sql table as a pandas dataframe.
        Parameters
        ----------
        table_name : str
        where_clause : Optional[str]
          If None, this method will pull the entire table. The user can set a
          custom clause if additional filtering is desired. Default is None.

        Returns
        -------
        pd.Dataframe
          A pandas dataframe created from the sql table.

        """
        with self._engine.begin() as conn:
            query = (
                f'SELECT * FROM "{table_name}"'
                if where_clause is None
                else f'SELECT * FROM "{table_name}" WHERE {where_clause}'
            )
            df = pd.read_sql_query(sql=text(query), con=conn)
        return df

    def execute_query(self, query: str) -> CursorResult:
        """
        Run a sql query against the database
        Parameters
        ----------
        query : str

        Returns
        -------
        CursorResult
          The result of the query.

        """
        with self._engine.begin() as conn:
            result = conn.execute(text(query))
        return result
