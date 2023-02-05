import pandas as pd
from time import time
from datetime import timedelta
import os
import argparse
from urllib.parse import urlparse
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3)
def extract_data(url):
    
    os.system(f'wget {url} -O output.csv.gz')
    os.system(f'gzip -d output.csv.gz')

    ny_taxi_df_iter = pd.read_csv('output.csv')

    return ny_taxi_df_iter

@task(log_prints=True, retries=3)
def transform_data(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, data): 
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as database_block:

        print('Inserting...')
        data.to_sql(name=table_name, con=database_block, if_exists="replace")
        print('Done!')


@flow(name="Ingest Flow")
def main():

    table_name = 'yellow_taxi_data'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'

    ny_taxi_df = extract_data(url) #here the iterator is not empty, already checked
    ny_taxi_df = transform_data(ny_taxi_df)
    ingest_data(table_name, ny_taxi_df)


if __name__ == '__main__':
    main()