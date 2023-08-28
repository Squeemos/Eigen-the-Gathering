"""Database manager tool. """

import argparse
import textwrap
import os
import re
import bz2
from functools import total_ordering

import pandas as pd

from database.db import ETGDatabase
from database.online import scry
from database.online.gcs import GCSConnection


@total_ordering
class DBFile:
    """Wrapper around database filename for easy manipulation."""
    def __init__(self, filename):
        components = re.split("[_.]", filename)

        self.filename = filename
        self.version = int(components[1][1:])

    def __eq__(self, other: object) -> bool:
        return self.version == other.version

    def __lt__(self, other: object) -> bool:
        return self.version < other.version


class DBManager:
    """Updates, compresses, and pushes/pulls databases."""
    def __init__(self, args, comp_type="bz2"):
        self.comp_type = comp_type
        self.args = args

    # Commands ------------------------

    def update(self):
        """Downloads data and creates/updates database."""
        path = "data/db/"

        dbfile = self._get_dbfile("db")

        # Download data
        # data = scry.download_default_cards()
        # data = scry.to_dataframe(data)
        temp_path = "database/experiments/data/"
        data = pd.read_json(temp_path + os.listdir(temp_path)[-2])

        if dbfile:
            fname = dbfile.filename
        else:
            fname = "etg_v1.db"

        db = ETGDatabase(path + fname)
        db.update(data)

    def increment(self):
        """Creates new version of database by copying."""
        path = "data/db/"

        dbfile = self._get_dbfile("db")

        if dbfile:
            # Open database
            db = ETGDatabase(path + dbfile.filename)

            # Open new database
            new_filename = f"etg_v{dbfile.version + 1}.db"
            db_new = ETGDatabase(path + new_filename)

            # Copy to new and set new as working db
            db.conn.backup(db_new.conn)
            db = db_new
        else:
            print("db-mgr: No database to increment!")

    def zip(self):
        dbfile = self._get_dbfile("db")

        if dbfile:
            zip_filename = dbfile.filename.split(".")[0] + ".bz2"

            with open(f"data/db/{dbfile.filename}", "rb") as f_in:
                with bz2.BZ2File(f"data/zip/{zip_filename}", "wb") as f_out:
                    f_out.writelines(f_in)
        else:
            print("db-mgr: No databases to zip!")

    def unzip(self):
        zipfile = self._get_dbfile("zip")

        if zipfile:
            db_filename = zipfile.filename.split(".")[0] + ".db"

            with bz2.BZ2File(f"data/zip/{zipfile.filename}", "rb") as f_in:
                with open(f"data/db/{db_filename}", "wb") as f_out:
                    f_out.writelines(f_in)
        else:
            print("db-mgr: No databases to unzip!")

    def pull(self):
        path = "data/zip/"

        gcs = GCSConnection()

        source_names = gcs.get_filenames()
        dbfile = self._get_dbfile(self.comp_type, source_names)

        if dbfile:
            fname = dbfile.filename
            fsize = self._get_filesize(path, fname)

            should_cont = input(f"db-mgr: Should '{fname}' [{fsize:.4f} Mb] be downloaded? (y,n) ").lower()
            if should_cont in ("y", "yes"):
                gcs.download(fname, path + fname)
        else:
            print("db-mgr: No databases to pull!")

    def push(self):
        path = "data/zip/"

        gcs = GCSConnection()

        dbfile = self._get_dbfile("zip")

        if dbfile:
            fname = dbfile.filename
            fsize = self._get_filesize(path, fname)

            should_cont = input(f"db-mgr: Should '{fname}' [{fsize:.4f} Mb] be pushed? (y,n) ").lower()
            if should_cont in ("y", "yes"):
                gcs.upload(path + fname, fname)

            # Delete older versions so only a given number remain
            gcs.clean(2)
        else:
            print("db-mgr: No zipped databases to push!")

    # Helpers -------------------------

    def _get_dbfile(self, ftype, filenames=None):
        v = self.args.version
        if v is None:
            return self._get_latest_dbfile(ftype, filenames)
        else:
            return self._get_version_dbfile(ftype, v)

    def _get_latest_dbfile(self, ftype: str, filenames = None):
        if filenames is None:
            path = f"data/{ftype}/"
            filenames = os.listdir(path)

        # Change file type to compression type if applicable
        if ftype == "zip":
            ftype = self.comp_type

        # Get newest version
        files = [DBFile(fname) for fname in filenames if f".{ftype}" in fname]

        if files:
            latest_file = max(files)

            return latest_file

        return None

    def _get_version_dbfile(self, ftype: str, version: int):
        path = f"data/{ftype}/"

        # Create filename for version
        fname = f"etg_v{version}."
        if ftype == "zip":
            ftype = self.comp_type
        fname += ftype

        if fname not in os.listdir(path):
            raise FileNotFoundError(f"There is no db version '{fname}'!")

        return DBFile(fname)

    @staticmethod
    def _get_filesize(path, fname):
        fsize = os.stat(path + fname).st_size

        # Bytes to Mb
        fsize = fsize / (1024 ** 2)

        return fsize


def main():
    # Parser
    descr = textwrap.dedent("""
        Manages ETG database updating and storage.

        Examples:
            $ py database/mgr.py update    -- updates latest db and creates new version
            $ py database/mgr.py zip -v 2  -- zip v2 in data/db/ to data/zip/
            $ py database/mgr.py push      -- push latest zipped db to online storage
    """)
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "command", 
        choices=["update", "increment", "zip", "unzip", "pull", "push"],
        help=(
            "Select a command for latest (default) or specified (-v) db version:\n"
            "  update    - Downloads daily data and creates/updates a db\n"
            "  increment - Copys db to create a new version\n"
            "  zip       - Compresses db and writes to data/zip/\n"
            "  unzip     - Uncompresses db and writes to data/db/\n"
            "  pull      - Pulls zipped db from online storage\n"
            "  push      - Pushes zipped db to online storage and deletes older versions if too many"
        )
    )
    parser.add_argument("-v", "--version", type=int, help="Version number to operate on")

    # Init
    args = parser.parse_args()
    cmd = args.command
    mgr = DBManager(args)
    db_alias = "latest db" if args.version is None else f"db v{args.version}"

    # Call command
    if cmd == "update":
        print(f"db-mgr: Updating {db_alias}...")
        mgr.update()
    if cmd == "increment":
        print(f"db-mgr: Creating new version of {db_alias}...")
        mgr.increment()
    if cmd == "zip":
        print(f"db-mgr: Zipping {db_alias}...")
        mgr.zip()
    if cmd == "unzip":
        print(f"db-mgr: Unzipping {db_alias}...")
        mgr.unzip()
    if cmd == "pull":
        print(f"db-mgr: Pulling {db_alias}...")
        mgr.pull()
    if cmd == "push":
        print(f"db-mgr: Pushing {db_alias}...")
        mgr.push()

if __name__ == "__main__":
    main()
