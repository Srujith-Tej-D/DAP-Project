from dagster import job
from extract import *
from datasets import *
from transform_n_load import *


@job
def my_data_pipeline():
    downloaded_data = saving_datasets()
    ds1, ds2 = ingesting_violations_json_to_mongo(downloaded_data), ingesting_crashes_csv_to_mongo(downloaded_data)
    violations_df, traffic_df = mongo_extraction(ds1, ds2)
    transform_and_load(violations_df, traffic_df)
