import os
import requests
from google.oauth2 import service_account
from google.cloud.exceptions import GoogleCloudError
from google.cloud import storage
from google.auth.exceptions import GoogleAuthError
from dotenv import load_dotenv
from time import sleep

from libs.logger import Logger

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")


class Module:
    """Class with functions"""

    def __init__(self) -> None:
        pass

    def get_storage_client(self):
        """Returns an instance of the Google Cloud Storage client"""
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS
        )
        return storage.Client(credentials=credentials)

    def download_file_api(self, url: str, save_path: str) -> None:
        """Download a file from an API and save it"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            with open(save_path, "wb") as file:
                file.write(response.content)

        except requests.exceptions.RequestException as excep:
            Logger.error(message=f"Error making request: {excep}")
        except IOError as excep:
            Logger.error(message=f"Error saving file: {excep}")

    def upload_file(
        self,
        save_path: str,
        source_file_name: str,
        destination_blob_name: str,
        bucket_name=BUCKET_NAME,
    ) -> None:
        """Upload a file to Google Cloud Storage"""
        try:
            Logger.info(message=f"Bucket name: {bucket_name}")
            Logger.info(message=f"Source file name: {source_file_name}")
            Logger.info(message=f"Destination blob name: {destination_blob_name}")

            storage_client = self.get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(os.path.join(destination_blob_name, source_file_name))
            blob.upload_from_filename(os.path.join(save_path, source_file_name))

            Logger.sucess(
                message=f"File saved at: gs://{bucket_name}/{destination_blob_name}"
            )

            # Check if the file exists before trying to remove it
            file_to_remove = os.path.join(save_path, source_file_name)
            if os.path.exists(file_to_remove):
                os.remove(file_to_remove)
                Logger.warning(
                    message=("------------------------------------------------")
                )
                sleep(2)
        except (GoogleAuthError, GoogleCloudError) as excep:  # More specific exceptions
            Logger.warning(message=f"Error saving file to bucket: {excep}")

    def download_and_upload_data(
        self,
        save_path: str,
        url_base: str,
        taxi_type: list,
        period: list,
        destination_blob_name: str,
    ) -> None:
        """Download and upload data for given taxi types and periods"""
        for tt in taxi_type:
            for date in period:
                url = f"{url_base}{tt}_tripdata_{date}.parquet"
                file_name = f"{tt}_tripdata_{date}.parquet"
                self.download_file_api(url, os.path.join(save_path, file_name))

                self.upload_file(
                    source_file_name=file_name,
                    destination_blob_name=destination_blob_name,
                    save_path=save_path,
                )
