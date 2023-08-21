#!/usr/bin/env python3
"""
    Magic card price tracking
"""

# Boilerplate
__author__ = "Ben"
__version__ = "1.0"
__license__ = "MIT"

from requests import get
import json
import pandas as pd

def download_data(url: str) -> pd.DataFrame:
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
    
    return pd.DataFrame.from_dict(json.loads(data.text))

def clean_data(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    small_df = pd.concat([df.drop(["prices"], axis=1), df["prices"].apply(pd.Series)], axis=1)
    return small_df[columns]

def main() -> int:
    df = download_data("https://api.scryfall.com/bulk-data/default_cards")

    columns = ["name", "set_name", "usd", "usd_foil", "usd_etched"]
    df = clean_data(df, columns)
    print(df)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())