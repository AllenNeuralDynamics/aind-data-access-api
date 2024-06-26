"""Module to interface with the Relational Database"""

from typing import Optional, Union

import pandas as pd
import sqlalchemy.engine
from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict
from sqlalchemy import create_engine, engine, text
from sqlalchemy.engine.cursor import CursorResult
from typing_extensions import Self

from aind_data_access_api.credentials import CoreCredentials


class RDSCredentials(CoreCredentials):
    """RDS db credentials"""

    model_config = SettingsConfigDict(env_prefix="RDS_")

    # Setting validation aliases for legacy purposes. Allows users
    # to use RDS_USER in addition to RDS_USERNAME as env vars
    username: str = Field(
        ...,
        validation_alias=AliasChoices("username", "RDS_USER", "RDS_USERNAME"),
    )
    password: SecretStr = Field(...)
    host: str = Field(...)
    port: int = Field(default=5432)
    dbname: Optional[str] = Field(default=None)

    @model_validator(mode="after")
    def validate_database_name(self) -> Self:
        """Sets database to db_name"""
        if self.database is None and self.dbname is None:
            raise ValueError(
                "At least one of dbname or database needs to be set"
            )
        elif self.database is None:
            self.database = self.dbname
        return self


class Client:
    """Class to establish a relational database client. Includes methods to
    read/write pandas dataframes to backend."""

    def __init__(
        self,
        credentials: RDSCredentials,
        drivername: Optional[str] = "postgresql",
    ):
        """
        Construct a client to interface with relational database.
        Parameters
        ----------
        credentials : CoreCredentials

        drivername: Optional[str]
            Combination of dialect[+driver] where the dialect is
            the database name such as ``mysql``, ``oracle``, ``postgresql``,
            etc. and the optional driver name is a DBAPI such as
            ``psycopg2``, ``pyodbc``, ``cx_oracle``, etc.

        """
        self.credentials = credentials
        self.drivername = drivername

    @property
    def _engine(self) -> sqlalchemy.engine.Engine:
        """Create a sqlalchemy engine:
        https://docs.sqlalchemy.org/en/20/core/engines.html

        Returns: sqlalchemy.engine.Engine
        """
        connection_url = engine.URL.create(
            drivername=self.drivername,
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
