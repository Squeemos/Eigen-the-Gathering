"""Wrapper around sqlite3 Connection with utilities for updating and easy querying."""

import sqlite3
import pandas as pd

from database.tables import cards, images, prices


class ETGDatabase:
    def __init__(self, database: str):
        self.conn = sqlite3.connect(database)

    def __del__(self):
        self.conn.close()

    def update(self, data):
        data["utc"] = pd.to_datetime("today").now()

        # Update tables
        cards.update(data, self.conn)
        images.update(data, self.conn)
        prices.update(data, self.conn)
