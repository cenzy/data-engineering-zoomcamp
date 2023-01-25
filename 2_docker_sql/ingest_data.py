import pandas as pd
from time import time
import os
import argparse
from urllib.parse import urlparse
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    os.system(f'wget {url} -O output.csv.gz')
    os.system(f'gzip -d output.csv.gz')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    ny_taxi_df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)

    for ny_taxi_df in ny_taxi_df_iter:
        t_start = time()

        ny_taxi_df.tpep_pickup_datetime = pd.to_datetime(ny_taxi_df.tpep_pickup_datetime)
        ny_taxi_df.tpep_dropoff_datetime = pd.to_datetime(ny_taxi_df.tpep_dropoff_datetime)

        ny_taxi_df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()
        print('Inserted a chunk, took %.3f second' %(t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV dato to Postgres")

    parser.add_argument('--user', type=str, default='root')
    parser.add_argument('--password', type=str, default='root')
    parser.add_argument('--host', type=str, default='pg-database')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--db', type=str, default='ny_taxi')
    parser.add_argument('--table_name', type=str, default='yellow_taxi_data')
    parser.add_argument('--url', type=str, default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz')

    args = parser.parse_args()

    main(args)