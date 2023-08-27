import sqlite3

import pandas as pd


def update(data: pd.DataFrame, conn: sqlite3.Connection):
    table_name = "Cards"

    df = data.loc[:, ["id", "name", "set_name"]]

    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT,
            name TEXT,
            set_name TEXT,
            PRIMARY KEY (id)
        )
    """)
    conn.commit()

    # Insert DataFrame into a temporary table
    df.to_sql(f"Temp{table_name}", conn, if_exists="replace", index=False)

    # Use SQL to merge the temporary table into the main table
    c.execute(f"""
        INSERT OR REPLACE INTO {table_name} (id, name, set_name)
        SELECT id, name, set_name FROM TempCards
    """)
    conn.commit()

    # Drop the temporary table
    c.execute(f"DROP TABLE Temp{table_name}")
    conn.commit()
