"""Module to interface with the Relational Database"""

from typing import Optional, Union
from typing_extensions import Self
import pandas as pd
from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict
import redshift_connector
import boto3
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
        s3_bucket_name: str,
        s3_key: str
    ):
        """
        Construct a client to interface with relational database.
        Parameters
        ----------
        credentials : CoreCredentials
        """
        self.credentials = credentials
        # TODO: s3_configs class and input with all this info
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.s3_client = boto3.client('s3')

    @property
    def _conn(self):
        """Create a redshift connector cursor.
        """
        connection = redshift_connector.connect(
            host=self.credentials.host,
            database=self.credentials.database,
            port=self.credentials.port,
            user=self.credentials.username,
            password=self.credentials.password.get_secret_value()
        )
        return connection

    def append_df_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
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
        with self._conn.cursor() as cursor:
            cursor.write_dataframe(df, table_name)

    def overwrite_table_with_df(
        self,
        df: pd.DataFrame,
        table_name: str,
    ) -> None:
        """
        Overwrite an existing table with a dataframe.
        Otherwise, creates table if it does not exist.
        Parameters
        ----------
        df : pd.Dataframe
        table_name : str
        Returns
        -------
        None
        """
        # copy dataframe to staging bucket
        csv_buffer = df.to_csv(index=False)  # Convert DataFrame to CSV string
        self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=self.s3_key, Body=csv_buffer)
        self.execute_query(query=f"DROP TABLE IF EXISTS {table_name}")
        self.execute_query(query=f"CREATE TABLE {table_name}")
        copy_query = f"COPY {table_name} FROM 's3://{self.s3_bucket_name}/{self.s3_key}' DELIMITER ',' CSV IGNOREHEADER 1;"
        self.execute_query(copy_query)
        # TODO: remove object from staging bucket
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
        with self._conn.cursor() as cursor:
            query = (
                f'SELECT * FROM "{table_name}"'
                if where_clause is None
                else f'SELECT * FROM "{table_name}" WHERE {where_clause}'
            )
            cursor.execute(query)
            data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    def execute_query(self, query: str) -> redshift_connector.Cursor:
        """
        Run a sql query against the database
        Parameters
        ----------
        query : str

        Returns
        -------
        Cursor
          The result of the query.

        """
        with self._conn.cursor() as cursor:
            result = cursor.execute(query)
        cursor.close()
        return result
