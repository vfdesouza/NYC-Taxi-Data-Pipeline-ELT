import os
from dotenv import load_dotenv
from libs.module import Module

load_dotenv()

func = Module()

URL_BASE = os.getenv("URL_BASE")


def main(year, month_start, month_end):
    period_list = [
        f"{year}-0{x}" if x < 10 else f"{year}-{x}"
        for x in range(month_start, month_end + 1)
    ]

    func.download_and_upload_data(
        url_base=URL_BASE,
        path_file="",
        taxi_type=[""],
        period=period_list,
        destination_blob_name="/",
    )


if __name__ == "__main__":
    print("Extraindo os dados da API NYC.")
    main(year=2022, month_start=1, month_end=12)
    print("Extração concluída!")
