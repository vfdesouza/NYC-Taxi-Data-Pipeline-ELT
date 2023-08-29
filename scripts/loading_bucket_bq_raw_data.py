import os
import pandas as pd
from datetime import datetime

from libs.module import Module
from libs.logger import Logger

func = Module()


def main(taxi_type, period_date, source_blob_name, destination_file_name):
    report = func.download_blob(
        taxi_type=taxi_type,
        period_date=period_date,
        source_blob_name=source_blob_name,
        destination_file_name=destination_file_name,
    )

    df = pd.read_parquet(path=report, engine="pyarrow")

    df["extract_at"] = datetime.now()

    func.load_table_from_dataframe_partitioning(
        table_name="raw_nyc_yellow",
        column_partitioning="tpep_pickup_datetime",
        columns_clustering=["VendorID"],
        dataframe=df,
    )

    os.remove(report)


if __name__ == "__main__":
    Logger.info(message="Carregando os dados na raw_zone do BigQuery...")
    main(
        taxi_type="yellow",
        period_date="2022-01",
        source_blob_name="NYC_YELLOW",
        destination_file_name="./data_temp/bucket_bq",
    )
    Logger.sucess(message="Carregamengo concluído!")
