"""Wrapper around sqlite3 Connection with utilities for updating and easy querying."""

import os
import sqlite3

import pandas as pd

from database.tables import cards, images, prices


class ETGDatabase:
    """Wrapper around sqlite db connection with updating/querying utilities"""

    all_table_names = ("Cards", "Images", "Prices")

    def __init__(self, database: str | None = None):
        # If no database given, use most recent db version in data folder
        if database is None:
            path = "data/db/"

            fnames = [fname for fname in os.listdir(path) if ".db" in fname]
            if not fnames:
                raise FileNotFoundError(f"No databases found in '{path}'!")

            database = max(fnames, key=lambda x: x.split("_")[1][1:])
            database = path + database

        self.conn = sqlite3.connect(database)

    def __del__(self):
        try:
            self.conn.close()
        except AttributeError:
            pass

    # Meta ----------------------------

    def info(self, head_size=3):
        """Displays an overview of each table in the database"""
        cursor = self.conn.cursor()

        for table in self.all_table_names:
            cursor.execute(f"SELECT * FROM {table} LIMIT {head_size}")

            print(f"=== {table} ===")

            col_names = [col_desc[0] for col_desc in cursor.description]
            print(col_names)

            rows = cursor.fetchall()
            for row in rows[:head_size]:
                print(row)

            row_count = cursor.execute(f"SELECT COUNT(id) FROM {table}").fetchall()[0][0]
            print(f"... ({row_count} rows total)", end="\n\n")

    # Querying ------------------------

    def query(self, query) -> pd.DataFrame:
        """Shorthand for SQL query."""
        return pd.read_sql(query, self.conn)

    def get_table(self, table_name: str) -> pd.DataFrame:
        """Returns the full table of the given name as a DataFrame."""
        return self.get_tables([table_name])

    def get_tables(self, table_names: list[str]) -> pd.DataFrame:
        """Inner joins all tables in given list and returns as DataFrame."""
        table_names = [name.title() for name in table_names if name in self.all_table_names]

        query = f"SELECT * FROM {table_names[0]}"
        for name in table_names[1:]:
            query += f" INNER JOIN {name} USING(id)"

        return pd.read_sql(query, self.conn)

    def get_all(self) -> pd.DataFrame:
        """Inner joins and returns all tables (expensive and likely unnecessary, see get_tables)."""
        return self.get_tables(self.all_table_names)
    
    def date_range(self, start: str, end: str | None = None) -> pd.DataFrame:
        """Returns Card/Price date between given range (inclusive).
        
        Format date strings as YYYY-MM-DD. If no end date given, defaults to today.
        """
        if end is None:
            end = self._current_date()

        return pd.read_sql(f"""
           SELECT * FROM Cards INNER JOIN Prices USING(id)
           WHERE utc BETWEEN '{start}' AND '{end}'
        """, self.conn)

    # Modification --------------------

    def update(self, df: pd.DataFrame, date: str | None = None):
        """Given an API response as a dictionary, creates/updates all tables"""
        if date is None:
            date = self._current_date()
        df["utc"] = date

        # Update tables
        try:
            cards.update(df, self.conn)
            images.update(df, self.conn)
            prices.update(df, self.conn)

            # Commit transaction
            self.conn.commit()
        except sqlite3.Error as err:
            print(f"An error occured: {err}")
            self.conn.rollback()

    # Modification --------------------

    @staticmethod
    def _current_date():
        return pd.Timestamp.utcnow().strftime('%Y-%m-%d')
