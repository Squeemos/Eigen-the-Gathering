import sqlite3

import pandas as pd


def update(data: pd.DataFrame, conn: sqlite3.Connection):
    table_name = "Images"

    df = pd.concat([data.loc[:, ["id", "image_status"]], pd.json_normalize(data.image_uris)], axis=1)

    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT, 
            image_status TEXT, 
            small TEXT, 
            normal TEXT, 
            large TEXT, 
            png TEXT, 
            art_crop TEXT, 
            border_crop TEXT,
            PRIMARY KEY (id)
        )
    """)
    conn.commit()

    # Insert DataFrame into a temporary table
    df.to_sql(f"Temp{table_name}", conn, if_exists="replace", index=False)

    # Use SQL to merge the temporary table into the main table
    c.execute(f"""
        INSERT OR REPLACE INTO {table_name} (id, image_status, small, normal, large, png, art_crop, border_crop)
        SELECT id, image_status, small, normal, large, png, art_crop, border_crop FROM TempImages
    """)
    conn.commit()

    # Drop the temporary table
    c.execute(f"DROP TABLE Temp{table_name}")
    conn.commit()
