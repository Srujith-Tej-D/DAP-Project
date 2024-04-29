from dagster import repository
from etl import *


@graph
def etl_pipeline():
    # Define dependencies
    downloaded_data = download_datasets_op()
    load_dataset_1_op(downloaded_data)
    load_dataset_2_op(downloaded_data)

# Job instance from the graph
etl_job = etl_pipeline.to_job()

@repository
def my_etl_repository():
    """
    Repository to hold ETL jobs for managing data extraction and ingestion.
    """
    return [downloading_datasets ,loading_dataset_1, loading_dataset_2]
