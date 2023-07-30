import os
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
from time import sleep

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = "my-datapipeline-394413-111b96604481.json"
BUCKET_NAME = "nyc-taxi-data-pipeline-elt"


class Module:
    def __init__(self) -> None:
        pass

    def get_storage_client(self):
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS
        )
        return storage.Client(credentials=credentials)

    def check_and_create_bucket_path(self, path, bucket_name=BUCKET_NAME):
        # Inicializa o cliente do Google Cloud Storage
        storage_client = self.get_storage_client()

        # Recupera o bucket
        bucket = storage_client.bucket(bucket_name)

        # Verifica se o caminho existe
        blob = bucket.blob(path)
        if blob.exists():
            print(f'O caminho "{path}" já existe no bucket "{bucket_name}".')
        else:
            # Cria o caminho se ele não existir (neste caso, é um diretório vazio)
            blob.upload_from_string(
                "", content_type="application/x-www-form-urlencoded;charset=UTF-8"
            )
            print(f'O caminho "{path}" foi criado no bucket "{bucket_name}".')

    def download_file_api(self, url: str, save_path: str) -> None:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            with open(save_path, "wb") as file:
                file.write(response.content)

        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição: {e}")
        except IOError as e:
            print(f"Erro ao salvar o arquivo: {e}")

    def upload_file(
        self,
        source_file_path: str,
        destination_blob_name: str,
        bucket_name=BUCKET_NAME,
    ) -> None:
        try:
            print("Bucket name:", bucket_name)
            print("Source file path:", source_file_path)
            print("Destination blob name:", destination_blob_name)

            storage_client = self.get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_path)

            print(f"Arquivo salvo em: gs://{bucket_name}/{destination_blob_name}")
            os.remove(source_file_path)
            print("-" * 60)
            sleep(2)
        except Exception as e:
            print(f"Erro ao salvar o arquivo: {e}")

    def download_and_upload_data(
        self,
        url_base: str,
        path_file: str,
        taxi_type: list,
        period: list,
        destination_blob_name: str,
    ) -> None:
        self.check_and_create_bucket_path(
            bucket_name=BUCKET_NAME, path=destination_blob_name
        )

        for tt in taxi_type:
            for date in period:
                url = f"{url_base}{tt}_tripdata_{date}.parquet"
                save_path = os.path.join(
                    ".", path_file, f"{tt}_tripdata_{date}.parquet"
                )
                self.download_file_api(url, save_path)

                # Remova a chamada para self.check_and_create_bucket_path()
                self.upload_file(
                    source_file_path=save_path,
                    destination_blob_name=destination_blob_name,
                    bucket_name=BUCKET_NAME,
                )
