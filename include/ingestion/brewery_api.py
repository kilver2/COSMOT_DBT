import requests
import pandas as pd


URL = "https://api.openbrewerydb.org/v1/breweries"


def fetch_breweries(URL) -> pd.DataFrame:
    response = requests.get(URL)
    response.raise_for_status()

    data = response.json()

    return pd.DataFrame(data)

