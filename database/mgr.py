"""Database manager tool. """

import argparse
import textwrap
import os
import re
import bz2
import logging
import sys
from functools import total_ordering

# import pandas as pd

from database.db import ETGDatabase
from database.online import scry
from database.online.gcs import GCSConnection


@total_ordering
class DBFile:
    """Wrapper around database filename for easy manipulation."""
    def __init__(self, filename: str):
        components = re.split("[_.]", filename)

        self.filename = filename
        self.version = int(components[1][1:])

    def __eq__(self, other: object) -> bool:
        return self.version == other.version

    def __lt__(self, other: object) -> bool:
        return self.version < other.version


class DBManager:
    """Updates, compresses, and pushes/pulls databases."""
    def __init__(self, args, logger, comp_type="bz2"):
        self.args = args
        self.logger = logger

        self.comp_type = comp_type

    # Commands ------------------------

    def update(self):
        """Downloads data and creates/updates database."""
        path = "data/db/"

        dbfile = self._get_dbfile("db")

        # Download data
        data = scry.download_default_cards()
        data = scry.to_dataframe(data)
        # temp_path = "database/experiments/data/"
        # data = pd.read_json(temp_path + os.listdir(temp_path)[-2])

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
            self._log_warning("db-mgr: No database to increment!")

    def zip(self):
        dbfile = self._get_dbfile("db")

        if dbfile:
            zip_filename = dbfile.filename.split(".")[0] + ".bz2"

            with open(f"data/db/{dbfile.filename}", "rb") as f_in:
                with bz2.BZ2File(f"data/zip/{zip_filename}", "wb") as f_out:
                    f_out.writelines(f_in)
        else:
            self._log_warning("db-mgr: No databases to zip!")

    def unzip(self):
        zipfile = self._get_dbfile("zip")

        if zipfile:
            db_filename = zipfile.filename.split(".")[0] + ".db"

            with bz2.BZ2File(f"data/zip/{zipfile.filename}", "rb") as f_in:
                with open(f"data/db/{db_filename}", "wb") as f_out:
                    f_out.writelines(f_in)
        else:
            self._log_warning("db-mgr: No databases to unzip!")

    def push(self):
        self.zip()

        path = "data/zip/"

        gcs = GCSConnection()

        dbfile = self._get_dbfile("zip")

        if dbfile:
            fname = dbfile.filename
            fsize = self._get_filesize(path, fname)

            print(f"db-mgr: Pushing '{fname}' [{fsize:.4f} Mb]...")

            gcs.upload(path + fname, fname)

            # Delete older versions so only a given number remain
            gcs.clean(3)
        else:
            self._log_warning("db-mgr: No zipped databases to push!")

    def pull(self):
        path = "data/zip/"

        gcs = GCSConnection()

        source_names = gcs.get_filenames()
        dbfile = self._get_dbfile(self.comp_type, source_names)

        if dbfile:
            fname = dbfile.filename

            print(f"db-mgr: Pulling '{fname}'...")

            gcs.download(fname, path + fname)

            self.unzip()
        else:
            self._log_warning("db-mgr: No databases to pull!")

    # Helpers -------------------------

    def _get_dbfile(self, ftype, filenames=None):
        """Gets latest or specific version as DBFile."""
        v = self.args.version
        if v is None:
            return self._get_latest_dbfile(ftype, filenames)
        else:
            return self._get_version_dbfile(ftype, v)

    def _get_latest_dbfile(self, ftype: str, filenames = None):
        """Helper method for getting latest as a DBFile."""
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
        """Helper method for getting a specific version as a DBFile."""
        path = f"data/{ftype}/"

        # Create filename for version
        fname = f"etg_v{version}."
        if ftype == "zip":
            ftype = self.comp_type
        fname += ftype

        if fname not in os.listdir(path):
            raise FileNotFoundError(f"There is no db version '{fname}'!")

        return DBFile(fname)

    def _log_warning(self, msg: str):
        print(msg)
        self.logger.warning(msg)

    @staticmethod
    def _get_filesize(path: str, fname: str):
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
    # Config argument parser
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
            "  push      - Zips and pushes db to online storage (deletes older versions if too many)"
            "  pull      - Pulls zipped db from online storage and unzips\n"
        )
    )
    parser.add_argument("-v", "--version", type=int, help="Version number to operate on")

    # Config logger
    logging.basicConfig(
        filename="error.log",
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s() -> %(message)s",
    )

    # Set logger to document uncaught errors
    sys.excepthook = lambda exc_type, exc_value, exc_traceback: logger.error(
        "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
    )

    # Create manager
    args = parser.parse_args()
    logger = logging.getLogger()
    mgr = DBManager(args, logger)

    # Other init
    cmd = args.command
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
    if cmd == "push":
        print(f"db-mgr: Pushing {db_alias}...")
        mgr.push()
    if cmd == "pull":
        print(f"db-mgr: Pulling {db_alias}...")
        mgr.pull()

if __name__ == "__main__":
    main()
