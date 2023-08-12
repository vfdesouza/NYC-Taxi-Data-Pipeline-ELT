import os
from dotenv import load_dotenv

from libs.module import Module
from libs.logger import Logger

load_dotenv()

func = Module()

URL_BASE = os.getenv("URL_BASE")


def main(year, month_start, month_end):
    period_list = [
        f"{year}-0{x}" if x < 10 else f"{year}-{x}"
        for x in range(month_start, month_end + 1)
    ]

    func.download_and_upload_data(
        save_path="./data_temp",
        url_base=URL_BASE,
        taxi_type=["yellow"],
        period=period_list,
        destination_blob_name="NYC_YELLOW/",
    )


if __name__ == "__main__":
    Logger.info(message="Extraindo os dados da API NYC.")
    main(year=2022, month_start=1, month_end=12)
    Logger.sucess(message="Extração concluída!")
