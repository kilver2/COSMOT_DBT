from datetime import datetime
import requests
import pandas as pd
import os
import logging as log

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


URL = "https://api.openbrewerydb.org/v1/breweries"


@dag(
    dag_id="brewery_ingestion",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def brewery_ingestion():

    @task()
    def fetch_breweries():
        response = requests.get(URL)
        response.raise_for_status()
        return response.json()

    @task()
    def load_to_databricks(data: list):
        from databricks import sql

        log.info(f"Starting Databricks load with {len(data)} records...")

        df = pd.DataFrame(data)
        df["_ingested_at"] = pd.Timestamp.now()

        log.info(f"DataFrame shape: {df.shape}")
        log.info(f"Columns: {list(df.columns)}")

        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_HOST").replace("https://", ""),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN"),
        )
        cursor = connection.cursor()
        log.info("Connected to Databricks successfully")

        catalog = "brewery"
        schema = "bronze"
        table = "raw_breweries"
        full_table_name = f"{catalog}.{schema}.{table}"

        # ✅ fully qualified catalog.schema
        log.info(f"Creating schema if not exists: {catalog}.{schema}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        log.info(f"Creating table if not exists: {full_table_name}")
        columns_sql = ", ".join([f"{col} STRING" for col in df.columns])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {columns_sql}
            )
            USING DELTA
        """)

        log.info(f"Inserting {len(df)} rows into {full_table_name}...")
        for i, (_, row) in enumerate(df.iterrows()):
            placeholders = ", ".join(["?"] * len(row))
            cursor.execute(
                f"INSERT INTO {full_table_name} VALUES ({placeholders})",
                tuple(str(v) for v in row),
            )
            if i % 50 == 0:
                log.info(f"Inserted {i}/{len(df)} rows...")

        cursor.close()
        connection.close()
        log.info(f"Done! Successfully loaded {len(df)} rows into {full_table_name}")

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_silver_gold",
        trigger_dag_id="dbt_brewery_silver_gold",  # must match dag_id in your dbt DAG file
        wait_for_completion=True,
    )

    # Chain: fetch → load → trigger dbt
    data = fetch_breweries()
    loaded = load_to_databricks(data)
    loaded >> trigger_dbt

brewery_ingestion()