#!/usr/bin/env python3
"""
    Magic card price tracking
"""

# Boilerplate
__author__ = "Ben"
__version__ = "1.0"
__license__ = "MIT"

from database.db import ETGDatabase

def main() -> int:
    # Simple example getting the database 
    db = ETGDatabase()
    df = db.get_tables(["Cards", "Prices"])
    print(df)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())