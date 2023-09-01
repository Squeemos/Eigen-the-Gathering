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

    mask = grief_table["set_name"].str.contains("Promos")
    promos = grief_table[mask]
    non_promos = grief_table[~mask]
    
    fig = plt.figure(figsize=(10, 10))
    plt.plot(non_promos["utc"], non_promos["usd"], label="Non-Foil")
    plt.plot(non_promos["utc"], non_promos["usd_foil"], label="Foil")
    plt.legend()
    plt.show()
    plt.close()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())