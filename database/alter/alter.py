"""FOR MODIFYING WORKING DATABASE! MAKE SURE YOU'VE TESTED!"""

import argparse
import textwrap

from database.db import ETGDatabase

def main():
    # Create parser
    descr = textwrap.dedent("""
        Alters most recent database. TEST FIRST!

        Examples:
            $ py database/alter/alter.py addcol Cards promo_type TEXT
            $ py database/alter/alter.py rmcol Prices tix
    """)
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Create subparsers for commands
    subparsers = parser.add_subparsers(title="subparsers", dest="command")
    parser_addcol = subparsers.add_parser("addcol",
        help="addcol <table_name> <col_name> <col_type> -- Adds a column")
    parser_rmcol = subparsers.add_parser("rmcol",
        help="rmcol <table_name> <col_name> -- Removes a column")
    subparsers.add_parser("vacuum",
        help="vacuum -- Reclaims space from deleted data")

    # Add args for subparsers
    for subparser in [parser_addcol, parser_rmcol]:
        subparser.add_argument("table_name", help="Name of SQLite table")
        subparser.add_argument("col_name", help="Name of column to alter")
    parser_addcol.add_argument("col_type", help="SQLite type, ex. TEXT")

    # Init db
    db = ETGDatabase()
    c = db.conn.cursor()

    # Get input
    args = parser.parse_args()
    cmd = args.command

    if cmd == "addcol":
        add_column(c, args.table_name, args.col_name, args.col_type)
    elif cmd == "rmcol":
        rm_col(c, args.table_name, args.col_name)
    elif cmd == "vacuum":
        vacuum_db(c)

    #c.commit()

def add_column(c, table_name, col_name, col_type):
    print(f"Adding '{col_name}' ({col_type}) to '{table_name}'...")

    c.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")

    print("Success!")

def rm_col(c, table_name, col_name):
    print(f"Removing '{col_name}' from '{table_name}'...")

    c.execute(f"ALTER TABLE {table_name} DROP {col_name}")

    print("Success!")

def vacuum_db(c):
    print("Vacuuming db...")

    c.execute("VACUUM")

    print("Success!")

if __name__ == "__main__":
    main()
