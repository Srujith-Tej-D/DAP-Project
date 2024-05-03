from dagster import job
from dagster import repository
from dagster import graph
from extract import *
from datasets import *


@graph
def etl_pipeline():
    downloaded_data = saving_datasets()
    ds1, ds2 = ingesting_violations_json_to_mongo(downloaded_data), ingesting_crashes_csv_to_mongo(downloaded_data)

etl_job = etl_pipeline.to_job()

@repository
def my_etl_repository():
    """
    Repository to hold ETL jobs for managing data extraction and ingestion.
    """
    return [etl_job]
