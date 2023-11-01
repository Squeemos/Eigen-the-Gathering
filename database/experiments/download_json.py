"""Downloads data for a date and saves in a json."""

import json
import datetime
from requests import get

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

    return json.loads(data.text)


def main() -> int:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"database/experiments/data/default-cards_{timestamp}.json"
    
    with open(filename, "w") as outfile:
        print ("Downloading data...")
        data = download_data("https://api.scryfall.com/bulk-data/default_cards")

        print("Writing data...")
        json.dump(data, outfile)

    print("Done.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
