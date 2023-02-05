from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-fcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="elevated-style-375519",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(year: int = 2021, month: int = 1, color: str = "yellow"):
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    print(len(df))
    write_bq(df)

@flow()
def etl_parent_flow(
    months = [1,2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == '__main__':
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)

if __name__ == '__main__':
    etl_gcs_to_bq()