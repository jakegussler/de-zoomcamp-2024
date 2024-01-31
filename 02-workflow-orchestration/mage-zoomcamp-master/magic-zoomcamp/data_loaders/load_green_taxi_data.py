import io
import pandas as pd
import requests
import datetime
from dateutil.relativedelta import relativedelta

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

    



@data_loader
def load_data_from_api(*args, **kwargs):

    lower_bound = kwargs['lower_bound']
    upper_bound = kwargs['upper_bound']
    
    start_date = datetime.datetime.strptime(lower_bound, "%Y-%m")
    end_date = datetime.datetime.strptime(upper_bound, "%Y-%m")

    consolidated_df = pd.DataFrame()

    taxi_dtypes = { 
        'VendorID' : pd.Int64Dtype(),
        'passenger_count' : pd.Int64Dtype(),
        'trip_distance' : float,
        'RatecodeID' : pd.Int64Dtype(),
        'store_and_fwd_flag' : str,
        'PULocationID' : pd.Int64Dtype(),
        'DOLocationID' : pd.Int64Dtype(),
        'payment_type' : pd.Int64Dtype(),
        'fare_amount' : float,
        'extra' : float,
        'mta_tax' : float,
        'tip_amount': float,
        'tolls_amount' : float,
        'improvement_surcharge' : float,
        'total_amount' : float,
        'congestion_surcharge' : float
    }

    parse_dates = ['lpep_pickup_datetime' , 'lpep_dropoff_datetime']
    
    current_date = start_date
    while current_date <= end_date:
        file_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{current_date.strftime('%Y-%m')}.csv.gz"
        df = pd.read_csv(file_url, sep = ',', compression='gzip', dtype = taxi_dtypes,parse_dates=parse_dates)
        consolidated_df = pd.concat([consolidated_df, df])

        current_date += relativedelta(months=1)

    return consolidated_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
