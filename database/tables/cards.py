import sqlite3

import pandas as pd

def list_to_str(data: pd.DataFrame, column_name: str):
    return data[column_name].apply(
        lambda x: ",".join(x) if isinstance(x, list) else None
    )

def update(data: pd.DataFrame, conn: sqlite3.Connection):
    table_name = "Cards"

    c = conn.cursor()

    # Create table if necessary
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT,
            name TEXT,
            set_name TEXT,
            border_color TEXT,
            promo_types TEXT,
            frame_effects TEXT,
            PRIMARY KEY (id)
        )
    """)

    features = ["id", "name", "set_name", "border_color",
                "promo_types", "frame_effects"]

    # Get columns needed for table from dataframe
    df = data.loc[:, features]

    # Convert list features to strings
    df["promo_types"] = list_to_str(df, "promo_types")
    df["frame_effects"] = list_to_str(df, "frame_effects")

    # Insert DataFrame into a temporary table
    df.to_sql(f"Temp{table_name}", conn, if_exists="replace", index=False)

    # Use SQL to merge the temporary table into the main table
    feats_sep = ", ".join(features)
    c.execute(f"""
        INSERT OR REPLACE INTO {table_name} ({feats_sep})
        SELECT {feats_sep} FROM TempCards
    """)

    # Drop the temporary table
    c.execute(f"DROP TABLE Temp{table_name}")

    c.close()
