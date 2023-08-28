"""Demo of database usage."""


from database.db import ETGDatabase

def main():
    # Can pass filename, but defaults to latest version in data/db/
    db = ETGDatabase()

    print("\n****************************************\n")

    # Prints summary showing contents
    print("<< db.info >>")
    db.info()

    print("\n****************************************\n")

    # Can do raw sql query
    print("<< db.query >>")
    df = db.query("SELECT name FROM Cards LIMIT 10")
    print(df)

    print("\n****************************************\n")

    # Can get one or more inner joined tables by passing
    print("<< db.get_tables >>")
    df = db.get_tables(["Cards", "Prices"])
    print(df.head())

    print("\n****************************************\n")

    print("<< db.date_range >>")
    df = db.date_range("2023-08-25", "2023-08-27")
    print(df.utc.unique())
    print(df.head())

    print("\n****************************************\n")

if __name__ == "__main__":
    main()
