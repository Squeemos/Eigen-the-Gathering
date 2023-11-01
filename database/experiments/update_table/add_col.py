"""FOR ADDING COLUMNS TO WORKING DATABASE! MAKE SURE YOU'VE TESTED!"""

from database.db import ETGDatabase

def main():
    table_name = "Cards"
    col_name = "frame_effects"
    col_type = "TEXT"

    db = ETGDatabase()
    c = db.conn.cursor()

    # Add column
    c.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")

    # Remove column
    #c.execute("ALTER TABLE {table_name} DROP {col_name}")

    #c.commit()

if __name__ == "__main__":
    main()
