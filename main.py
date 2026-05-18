from include.ingestion.brewery_api import fetch_breweries
from include.ingestion.databricks_loader import load_to_databricks


URL = "https://api.openbrewerydb.org/v1/breweries"


df = fetch_breweries(URL)

print(df.head())


load_to_databricks(
    df,
    catalog="brewery",
    schema="bronze",
    table="raw_breweries"
)