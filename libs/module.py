import os
import requests
import pandas as pd

from google.oauth2 import service_account
from google.cloud.exceptions import GoogleCloudError, ClientError
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery
from google.auth.exceptions import GoogleAuthError
from dotenv import load_dotenv
from time import sleep

from libs.logger import Logger

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DATASET_RAW_ZONE = os.getenv("DATASET_RAW_ZONE")
BUCKET_NAME = os.getenv("BUCKET_NAME")


class Module:
    """Class with functions"""

    def __init__(self) -> None:
        pass

    def get_storage_client(self) -> storage.Client | None:
        """Returns an instance of the Google Cloud Storage client"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS
            )

            return storage.Client(credentials=credentials)

        except (GoogleAuthError, ClientError) as excep:
            Logger.warning(
                message=f"Error launching Google Cloud Storage client instance: {excep}"
            )

            return None

    def get_bigquery_client(self) -> bigquery.Client | None:
        """Returns an instance of the Google BigQuery client"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

            return bigquery.Client(
                credentials=credentials, project=credentials.project_id
            )

        except (GoogleAuthError, ClientError) as excep:
            Logger.warning(
                message=f"Error launching Google Bigquery client instance: {excep}"
            )

            return None

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

            file_to_remove = os.path.join(save_path, source_file_name)
            if os.path.exists(file_to_remove):
                os.remove(file_to_remove)
                Logger.warning(
                    message=("------------------------------------------------")
                )
                sleep(2)
        except (GoogleAuthError, GoogleCloudError) as excep:
            Logger.warning(message=f"Error saving file to bucket: {excep}")

    def download_and_upload_data(
        self,
        save_path: str,
        url_base: str,
        types_of_taxis: list,
        period: list,
        destination_blob_name: str,
    ) -> None:
        """Download and upload data for given taxi types and periods"""
        for taxi_type in types_of_taxis:
            for date in period:
                url = f"{url_base}{taxi_type}_tripdata_{date}.parquet"
                file_name = f"{taxi_type}_tripdata_{date}.parquet"
                self.download_file_api(url, os.path.join(save_path, file_name))

                self.upload_file(
                    source_file_name=file_name,
                    destination_blob_name=destination_blob_name,
                    save_path=save_path,
                )

    def download_blob(
        self,
        taxi_type: str,
        period_date: str,
        source_blob_name: str,
        destination_file_name: str,
        bucket_name: str | None = BUCKET_NAME,
    ) -> str:
        """Downloads a blob from the bucket."""
        storage_client = self.get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        file_name = f"{taxi_type}_tripdata_{period_date}.parquet"
        blob = bucket.blob(f"{source_blob_name}/{file_name}")

        blob.download_to_filename(os.path.join(destination_file_name, file_name))

        Logger.info(
            message=f"Arquivo {source_blob_name} baixado para {destination_file_name}"
        )

        return os.path.join(destination_file_name, file_name)

    def load_table_from_dataframe_partitioning(
        self,
        table_name: str,
        column_partitioning: str,
        columns_clustering: list,
        dataframe: pd.DataFrame,
    ):
        """Loading partitioned data into BigQuery raw_zone"""
        # Set up the BigQuery client
        bigquery_client = self.get_bigquery_client()

        # Define the table partitioning and clustering options
        time_partitioning = bigquery.TimePartitioning(
            field=column_partitioning,  # specify the column to use for partitioning
            type_=bigquery.TimePartitioningType.DAY,  # partition by day
            expiration_ms=None,  # never auto-expire partitions
        )
        clustering_fields = (
            columns_clustering  # specify the columns to use for clustering
        )

        # Define the table reference
        table_ref = bigquery_client.dataset(DATASET_RAW_ZONE).table(table_name)

        # schema = [
        #     bigquery.SchemaField(
        #         column, "DATETIME" if column == "extract_at" else "STRING"
        #     )
        #     for column in dataframe.columns
        # ]

        # Define the table definition with partitioning and clustering options

        table = bigquery.Table(table_ref)  # , schema=schema)
        table.time_partitioning = time_partitioning
        table.clustering_fields = clustering_fields

        try:
            bigquery_client.get_table(table_ref)
            return Logger.warning(message=f"Table {table_name} already exists.")
        except NotFound:
            Logger.warning(message=f"Table {table_name} does not exist. Creating...")

        parquet_options = bigquery.ParquetOptions()
        parquet_options.enable_list_inference = False

        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(column_partitioning, "DATETIME")],
            write_disposition="WRITE_APPEND",
            ignore_unknown_values=True,
            time_partitioning=table.time_partitioning,
            clustering_fields=table.clustering_fields,
            parquet_options=parquet_options,
        )

        job = bigquery_client.load_table_from_dataframe(
            dataframe, table_ref, job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        # Print the number of rows inserted
        Logger.sucess(message=f"Loaded {job.output_rows} rows into {table.path}")
