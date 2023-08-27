import sqlite3

import pandas as pd


def update(data: pd.DataFrame, conn: sqlite3.Connection):
    table_name = "Prices"

    df = pd.concat([data.loc[:, ["id", "utc"]], pd.json_normalize(data.prices)], axis=1)

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
    conn.commit()

    # Append DataFrame data to table
    df.to_sql(table_name, conn, if_exists='append', index=False)

    # Delete data older than a month
    c.execute(f"DELETE FROM {table_name} WHERE utc <= datetime('now', '-1 month', 'utc')")
    conn.commit()
