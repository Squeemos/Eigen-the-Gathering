
import os

import pandas as pd

from database.db import ETGDatabase

def main():
    path = "database/experiments/data/"

    db = ETGDatabase(path + "test.db")

    filenames = [js for js in os.listdir(path) if ".json" in js]

    for filename in filenames:
        data = pd.read_json(path + filename)
        db.update(data)

    db.info()

    big_df = db.get_all()
    print(big_df.info())
    print(big_df.shape)
    print(big_df.head())

if __name__ == "__main__":
    main()
