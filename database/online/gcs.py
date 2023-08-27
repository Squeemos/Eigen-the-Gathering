import os

from google.cloud import storage


class GCSConnection:
    def __init__(self):
        keypath = "database/online/keys/gcs/"
        keyfile = os.listdir(keypath)[-1]

        self.client = storage.Client.from_service_account_json(os.path.abspath(keypath + keyfile))
        self.bucket_name = "etg-data"
        self.bucket = self.client.bucket(self.bucket_name)

    def download(self, source_bname, dest_fname):
        blob = self.bucket.blob(source_bname)
        blob.download_to_filename(dest_fname)

    def upload(self, source_fname, dest_bname):
        blob = self.bucket.blob(dest_bname)

        with open(source_fname, "rb") as f:
            blob.upload_from_file(f)

    def get_filenames(self):
        return [blob.name for blob in self.bucket.list_blobs()]
