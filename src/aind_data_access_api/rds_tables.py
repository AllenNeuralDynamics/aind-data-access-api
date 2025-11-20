"""Module to interface with the Relational Database"""

from typing import Optional, Union

import pandas as pd
import redshift_connector
from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict
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
        drivername: Optional[str] = None,
    ):
        """
        Construct a client to interface with relational database.
        Parameters
        ----------
        credentials : CoreCredentials

        drivername: Optional[str]
            Deprecated parameter, kept for backward compatibility.
            Ignored when using redshift-connector.

        """
        self.credentials = credentials
        if drivername is not None:
            import warnings
            warnings.warn(
                "drivername parameter is deprecated and ignored when using "
                "redshift-connector",
                DeprecationWarning,
                stacklevel=2
            )

    def _get_connection(self) -> redshift_connector.Connection:
        """Create a redshift-connector connection.

        Returns
        -------
        redshift_connector.Connection
            A connection to the Redshift database.
        """
        return redshift_connector.connect(
            host=self.credentials.host,
            port=self.credentials.port,
            database=self.credentials.database,
            user=self.credentials.username,
            password=self.credentials.password.get_secret_value(),
        )

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
            Note: dtype parameter is kept for backward compatibility
            but is not directly supported by redshift-connector.

        Returns
        -------
        None

        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            # Create INSERT statements from dataframe
            columns = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = (
                f'INSERT INTO "{table_name}" ({columns}) '
                f'VALUES ({placeholders})'
            )
            
            # Execute batch insert
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            
            conn.commit()
        finally:
            conn.close()
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
            Note: dtype parameter is kept for backward compatibility
            but is not directly supported by redshift-connector.

        Returns
        -------
        None
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            # Drop and recreate table
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            
            # Create table with columns from dataframe
            column_defs = []
            for col in df.columns:
                # Infer SQL type from pandas dtype
                dtype_str = self._infer_sql_type(df[col].dtype)
                column_defs.append(f'"{col}" {dtype_str}')
            
            create_query = (
                f'CREATE TABLE "{table_name}" '
                f'({", ".join(column_defs)})'
            )
            cursor.execute(create_query)
            
            # Insert data
            columns = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = (
                f'INSERT INTO "{table_name}" ({columns}) '
                f'VALUES ({placeholders})'
            )
            
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            
            conn.commit()
        finally:
            conn.close()
        return None

    def _infer_sql_type(self, dtype) -> str:
        """
        Infer SQL type from pandas dtype.
        
        Parameters
        ----------
        dtype : pandas dtype
        
        Returns
        -------
        str
            SQL type string
        """
        dtype_str = str(dtype)
        if "int" in dtype_str:
            return "INTEGER"
        elif "float" in dtype_str:
            return "FLOAT"
        elif "bool" in dtype_str:
            return "BOOLEAN"
        elif "datetime" in dtype_str:
            return "TIMESTAMP"
        elif "object" in dtype_str:
            return "VARCHAR(MAX)"
        else:
            return "VARCHAR(MAX)"

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
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            query = (
                f'SELECT * FROM "{table_name}"'
                if where_clause is None
                else f'SELECT * FROM "{table_name}" WHERE {where_clause}'
            )
            cursor.execute(query)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create dataframe
            df = pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()
        return df

    def execute_query(self, query: str) -> redshift_connector.Cursor:
        """
        Run a sql query against the database
        Parameters
        ----------
        query : str

        Returns
        -------
        redshift_connector.Cursor
          The cursor object after executing the query.
          Note: The connection is closed after query execution,
          so fetch results before returning if needed.

        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
        finally:
            conn.close()
        return cursor
