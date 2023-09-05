#!/usr/bin/env python3
"""
    Running Auto ARIMA on cards
"""

# Boilerplate
__author__ = "Ben"
__version__ = "1.0"
__license__ = "MIT"

from database.db import ETGDatabase

from matplotlib import pyplot as plt

def main() -> int:
    db = ETGDatabase()
    df = db.get_tables(["Cards", "Prices"])

    grief_table = df[(df["name"] == "Grief")]
    grief_table = grief_table[grief_table["set_name"].str.contains("Modern Horizons 2")]

    fig = plt.figure(figsize=(10, 10))
    for card_id in grief_table["id"].unique():
        small_table = grief_table[grief_table["id"] == card_id]

        # Here's where to include the borderless information, todo later
        if not small_table["usd"].isna().all():
            plt.plot(small_table["utc"], small_table["usd"], label=f"{card_id}")

        if not small_table["usd_foil"].isna().all():
            plt.plot(small_table["utc"], small_table["usd_foil"], label=f"{card_id} (Foil)")

        if not small_table["usd_etched"].isna().all():
            plt.plot(small_table["utc"], small_table["usd_etched"], label=f"{card_id} (Foil Etched)")

    plt.legend()
    plt.show()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())