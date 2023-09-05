import sqlite3

import pandas as pd


def update(data: pd.DataFrame, conn: sqlite3.Connection):
    table_name = "Prices"

    # Manually create a table with a primary key if it doesn't exist
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT,
            utc TEXT,
            usd REAL,
            usd_foil REAL,
            usd_etched REAL,
            eur REAL,
            eur_foil REAL,
            tix REAL,
            PRIMARY KEY(id, utc)
        )
    """)

    # Create an index on date if not already done
    try:
        c.execute(f"CREATE INDEX idx_{table_name}_utc ON {table_name}(utc)")
    except sqlite3.OperationalError:
        pass

    # Get columns needed for table from dataframe
    df = pd.concat([data.loc[:, ["id", "utc"]], pd.json_normalize(data.prices)], axis=1)

    # Append DataFrame data to table
    df.to_sql(table_name, conn, if_exists='append', index=False)

    # Delete data older than a month
    c.execute(f"DELETE FROM {table_name} WHERE utc <= datetime('now', '-1 month', 'utc')")

    c.close()
