"""Database update tool."""

import argparse
import os
import re
import bz2
from functools import total_ordering

import pandas as pd

from database.db import ETGDatabase
from database.comm import scry


@total_ordering
class DBFile:
    def __init__(self, filename):
        components = re.split("[_.]", filename)

        self.filename = filename
        self.version = int(components[1][1:])
        #self.filetype = components[-1]

    def __eq__(self, other: object) -> bool:
        return self.version == other.version

    def __lt__(self, other: object) -> bool:
        return self.version < other.version


class DBManager:
    def __init__(self, increment: bool):
        self.comp_type = "bz2"
        self.increment = increment

    def update(self):
        # Download data
        #data = scry.download_default_cards()
        #data = scry.to_dataframe(data)
        path = "database/experiments/data/"
        data = pd.read_json(path + os.listdir(path)[-2])

        path = "data/db/"

        latest_file = self._get_latest("db")

        if latest_file:
            # Open most recent version
            db = ETGDatabase(path + latest_file.filename)

            # If creating new version, copy into new database and set as db to be updated
            if self.increment:
                new_filename = f"etg_v{latest_file.version + 1}.db"
                db_new = ETGDatabase(path + new_filename)
                db.conn.backup(db_new.conn)
                db = db_new
        else:
            new_filename = "etg_v1.db"
            db = ETGDatabase(path + new_filename)

        db.update(data)

    def zip(self):
        latest_db = self._get_latest("db")

        if latest_db:
            zip_filename = latest_db.filename.split(".")[0] + ".bz2"

            with open(f"data/db/{latest_db.filename}", "rb") as f_in:
                with bz2.BZ2File(f"data/zip/{zip_filename}", "wb") as f_out:
                    f_out.writelines(f_in)
        else:
            print("No databases to zip!")

    def unzip(self):
        latest_zip = self._get_latest("zip")

        if latest_zip:
            db_filename = latest_zip.filename.split(".")[0] + ".db"

            with bz2.BZ2File(f"data/zip/{latest_zip.filename}", "rb") as f_in:
                with open(f"data/db/{db_filename}", "wb") as f_out:
                    f_out.writelines(f_in)
        else:
            print("No databases to unzip!")

    @staticmethod
    def pull():
        pass

    @staticmethod
    def push():
        pass

    def _get_latest(self, ftype: str):
        path = f"data/{ftype}/"

        if ftype == "zip":
            ftype = self.comp_type

        # Get newest version
        files = [DBFile(fname) for fname in os.listdir(path) if f".{ftype}" in fname]

        if files:
            latest_file = max(files, key=lambda x: x.version)

            return latest_file

        return None


def main():
    parser = argparse.ArgumentParser(description="Manages ETG database updating and storage.")
    parser.add_argument("command", choices=["update", "zip", "unzip", "pull", "push"])
    parser.add_argument("-i", "--increment", action="store_true", help="Creates a new version during action.")
    #parser.add_argument("--option", type=str, help="Option supplied to command.")
    args = parser.parse_args()

    cmd = args.command
    #cmd = "zip"

    mgr = DBManager(args.increment)

    if cmd == "update":
        print("dbup: Updating latest db...")
        mgr.update()
    if cmd == "zip":
        print("dbup: Zipping latest db...")
        mgr.zip()
    if cmd == "unzip":
        print("dbup: Unzipping latest db...")
        mgr.unzip()
    if cmd == "pull":
        print("dbup: Pulling newest db...")
        mgr.pull()
    if cmd == "push":
        print("dbup: Pushing newest db...")
        mgr.push()

if __name__ == "__main__":
    main()
    