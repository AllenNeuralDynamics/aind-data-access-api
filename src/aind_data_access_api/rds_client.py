import csv
from io import StringIO

import pandas as pd
from sqlalchemy import create_engine


class Client:
    def __init__(self, user, password, database, host, port=5432):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    @property
    def engine(self):
        return create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:"
            f"{self.port}/{self.database}"
        )

    @staticmethod
    def __psql_insert_copy(table, conn, keys, data_iter):
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = "{}.{}".format(table.schema, table.name)
            else:
                table_name = table.name

            sql = "COPY {} ({}) FROM STDIN WITH CSV".format(
                table_name, columns
            )
            cur.copy_expert(sql=sql, file=s_buf)

    def append_df_to_table(self, df, table_name: str) -> None:
        df.to_sql(
            table_name,
            self.engine,
            method=self.__psql_insert_copy,
            if_exists="append",
        )
        return None

    def overwrite_table_with_df(self, df, table_name: str) -> None:
        df.to_sql(
            table_name,
            self.engine,
            method=self.__psql_insert_copy,
            if_exists="replace",
        )
        return None

    def read_table(self, table_name: str) -> pd.DataFrame:
        return pd.read_sql_query(
            f'SELECT * FROM "{table_name}"', con=self.engine
        )
