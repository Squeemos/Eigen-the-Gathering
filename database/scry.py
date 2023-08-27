"""Utilities for downloading and processing data via the Scryfall API."""

from typing import Union

import json
import datetime
from requests import get

import pandas as pd


def download_default_cards() -> dict:
    """Downloads default cards and returns response as a dict."""
    url = "https://api.scryfall.com/bulk-data/default_cards"

    print ("scry: Downloading default cards...")

    data = get(url)
    if not data.ok:
        raise RuntimeError(f"Request failed, got: {data.status_code}")

    try:
        cards_url = json.loads(data.text)["download_uri"]
    except Exception as e:
        raise RuntimeError(f"Exception loading data url. {e}")

    data = get(cards_url)
    if not data.ok:
        raise RuntimeError(f"Request failed, got: {data.status_code}")

    print("scry: Done.")

    # Convert text to dictionary and return
    return json.loads(data.text)

def save_json(data: dict, filepath: Union[str, None] = None):
    """Save API response dictionary as a json."""
    if filepath is None:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filepath = f"data/json/default-cards_{timestamp}.json"

    print("scry: Writing data as json to '{filepath}'...")

    with open(filepath, "w") as outfile:
        json.dump(data, outfile)

    print("scry: Done.")

def to_dataframe(data: dict) -> pd.DataFrame:
    """Converts API response dict to a Pandas DataFrame."""
    df = pd.DataFrame.from_dict(data)

    return df
