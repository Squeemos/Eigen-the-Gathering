import os

import pandas as pd

from database.db import ETGDatabase

def main():
    db = ETGDatabase("data/db/etg_v1.db")

    path = "database/experiments/data/"
    filenames = [js for js in os.listdir(path) if ".json" in js]

    for filename in filenames:
        data = pd.read_json(path + filename)
        db.update(data, filename[14:24])

if __name__ == "__main__":
    main()
