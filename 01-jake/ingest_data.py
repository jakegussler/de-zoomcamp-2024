#!/usr/bin/env python
# coding: utf-8


import argparse

import os

import pandas as pd

from sqlalchemy import create_engine
from time import time

def main(params):
    print('Running Main Function...Assigning Parameters...')
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_gz_name = 'output.csv.gz'
    csv_name = 'output.csv'

    print('Creating Engine...')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print('Attempting to connect to engine...')
    engine.connect()


    print('Engine Connected... Obtaining CSV from GitHub...l')
    
    # download the csv 
    os.system(f'wget {url} -O {csv_gz_name}')

    print('Downloaded CSV...Unzipping...')

    os.system(f'gunzip -k {csv_gz_name}')
    
    print(f'{csv_name} unzipped...')
    
    df = pd.read_csv(csv_name, nrows = 100)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print('Adding header to table')
    df.head(n=0).to_sql(name= table_name, con=engine, if_exists = 'replace')

    print('Building iterator')
    df_iter = pd.read_csv(csv_name, iterator = True, chunksize = 100000)

    print('Appending 1st 100 rows')
    df.to_sql(name='yellow_taxi_data',con=engine, if_exists='append')

    print('Starting Append loop...')
    while True:
        
        try:
            
            t_start = time()
            
            print('Next iteration...')
            df = next(df_iter)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            
            print('Another chunk added....took %.3f seconds' % (t_end - t_start))
            
        except StopIteration:
            
            print ('all chunks processed')
            return 0;
            break;



if __name__ == '__main__':

    print('Initializing parser...')
    parser = argparse.ArgumentParser(description='Ingest CSV Data to Postgres')

    print('Adding Parser Arguments...')    
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write data to')
    parser.add_argument('--url', help='url of the csv file')
    parser.add_argument('--network', help='name of the network')

    print('Parsing args...')
    args = parser.parse_args()

    print('Running main with args...')
    main(args)



