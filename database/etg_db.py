"""Wrapper around sqlite3 Connection with utilities for updating and easy querying."""

import sqlite3
import pandas as pd

from database.tables import cards, images, prices


class ETGDatabase:
    """Wrapper around sqlite db connection with updating/querying utilities"""

    all_table_names = ("Cards", "Images", "Prices")

    def __init__(self, database: str):
        self.conn = sqlite3.connect(database)

    def __del__(self):
        self.conn.close()

    # Meta ----------------------------

    def info(self, head_size=3):
        """Displays an overview of each table in the database"""
        cursor = self.conn.cursor()

        for table in self.all_table_names:
            cursor.execute(f"SELECT * FROM {table}")

            print(f"=== {table} ===")

            col_names = [col_desc[0] for col_desc in cursor.description]
            print(col_names)

            rows = cursor.fetchall()
            row_count = cursor.execute(f"SELECT COUNT(id) FROM {table}").fetchall()[0][0]
            for row in rows[:head_size]:
                print(row)

            print(f"... ({row_count} rows total)", end="\n\n")

    # Querying ------------------------

    def get_table(self, table_name: str):
        """Returns the full table of the given name as a DataFrame"""
        return self.get_tables((table_name))
    
    def get_tables(self, table_names: list[str]):
        """Inner joins all tables in given list and returns as DataFrame"""
        table_names = [name.title() for name in table_names if name in self.all_table_names]

        query = f"SELECT * FROM {table_names[0]}"
        for name in table_names[1:]:
            query += f" INNER JOIN {name} USING(id)"

        return pd.read_sql(query, self.conn)

    def get_all_tables(self):
        """Inner joins and returns all tables (expensive and likely unnecessary, see get_tables)"""
        return self.get_tables(self.all_table_names)

    # Modification --------------------

    def update(self, df: dict):
        """Given an API response as a dictionary, creates/updates all tables"""
        df["utc"] = pd.to_datetime("today").now()

        # Update tables
        cards.update(df, self.conn)
        images.update(df, self.conn)
        prices.update(df, self.conn)
