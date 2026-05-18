from databricks import sql
from dotenv import load_dotenv

import os
import pandas as pd


load_dotenv()


def load_to_databricks(
    df,
    catalog,
    schema,
    table
):

    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST").replace("https://", ""),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

    cursor = connection.cursor()

    full_table_name = f"{catalog}.{schema}.{table}"

    cursor.execute(
        f"CREATE SCHEMA IF NOT EXISTS {schema}"
    )

    df["_ingested_at"] = pd.Timestamp.now()

    columns = []

    for column in df.columns:
        columns.append(f"{column} STRING")

    columns_sql = ", ".join(columns)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {columns_sql}
        )
    """)

    for _, row in df.iterrows():

        placeholders = ", ".join(["?"] * len(row))

        query = f"""
            INSERT INTO {full_table_name}
            VALUES ({placeholders})
        """

        cursor.execute(
            query,
            tuple(str(value) for value in row)
        )

        

    print(f"Loaded into {full_table_name}")