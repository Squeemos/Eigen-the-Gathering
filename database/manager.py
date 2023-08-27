"""Database update tool."""

import argparse
import os
import re
import json

import pandas as pd

from database.etg_db import ETGDatabase
from database import scry


class DBFile:
    def __init__(self, filename):
        components = re.split("[_.]", filename)

        self.filename = filename
        self.version = int(components[1][1:])


class DBManager:
    def __init__(self) -> None:
        pass

    @staticmethod
    def pull():
        pass

    @staticmethod
    def update(new_version=True):
        # Download data
        #data = scry.download_default_cards()
        path = "database/experiments/data/"
        data = pd.read_json(path + os.listdir(path)[-2])

        path = "data/db/"

        # Get newest version
        files = [DBFile(fname) for fname in os.listdir(path) if ".db" in fname]

        if files:
            latest_file = max(files, key=lambda x: x.version)
            latest_filename = path + latest_file.filename

            # Open most recent version
            db = ETGDatabase(latest_filename)

            # If creating new version, copy into new database and set as db to be updated
            if new_version:
                new_filename = path + f"etg_v{latest_file.version + 1}.db"
                db_new = ETGDatabase(new_filename)
                db.conn.backup(db_new.conn)
                db = db_new
        else:
            new_filename = path + "etg_v1.db"
            db = ETGDatabase(new_filename)

        db.update(data)

    @staticmethod
    def push():
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["pull", "update", "push"])
    args = parser.parse_args()

    cmd = args.command

    mgr = DBManager()

    if cmd == "pull":
        print("dbup: Pulling newest db...")
        mgr.pull()
    if cmd == "update":
        print("dbup: Updating latest db...")
        mgr.update()
    if cmd == "push":
        print("dbup: Pushing newest db...")
        mgr.push()

if __name__ == "__main__":
    main()
    