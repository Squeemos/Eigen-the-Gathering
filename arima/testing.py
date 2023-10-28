#!/usr/bin/env python3
"""
    Module docstring
"""

# Boilerplate
__author__ = "Your Name"
__version__ = "1.0"
__license__ = "MIT"

from requests import get
import pandas as pd


def download_default_cards() -> dict:
    """ONLY ONCE A DAY! Downloads default cards and returns response as a dict"""
    url = "https://api.scryfall.com/bulk-data/default_cards"

    print ("scry: Downloading default cards...")

    data = get(url)
    if not data.ok:
        raise RuntimeError(f"Request failed, got: {data.status_code}")

    try:
        cards_url = data.json()["download_uri"]
    except Exception as e:
        raise RuntimeError(f"Exception loading data url. {e}")

    data = get(cards_url)
    if not data.ok:
        raise RuntimeError(f"Request failed, got: {data.status_code}")

    print("scry: Done.")

    # Convert text to dictionary and return
    return data.json()

def main() -> int:
    df = pd.DataFrame(download_default_cards())
    df = df[df["name"] == "Skithiryx, the Blight Dragon"]
    df.to_csv("./skith.csv")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())