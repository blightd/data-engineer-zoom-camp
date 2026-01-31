#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine

year = 2021
month = 1
pd.__file__
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = (f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz")
url

df = pd.read_csv(url)

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates
)

get_ipython().system('uv add sqlalchemy')

get_ipython().system('uv add psycopg --binary')


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

get_ipython().system('uv add psycopg2 --binary')

print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator = True,
    chunksize = 100000,
)

get_ipython().system('uv add tqdm')



for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data',con=engine, if_exists = 'append')




