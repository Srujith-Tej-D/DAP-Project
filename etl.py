from dagster import job
from dagster import repository
from dagster import graph
from extract import *
from datasets import *

<<<<<<< Updated upstream
@job
def jobs():
    #All jobs
    extract_and_store_violations_in_mongo()
=======



@op
def downloading_datasets():
    saving_datasets()

@op
def loading_dataset_1(downloaded_data):
    extract_and_store_violations_in_mongo()

@op
def loading_dataset_2(downloaded_data):
    ingest_csv_to_mongo()

@graph
def etl_pipeline():
    downloaded_data = downloading_datasets()
    loading_dataset_1(downloaded_data)
    loading_dataset_2(downloaded_data)

etl_job = etl_pipeline.to_job()

@repository
def my_etl_repository():
    """
    Repository to hold ETL jobs for managing data extraction and ingestion.
    """
    return [etl_job]
>>>>>>> Stashed changes
