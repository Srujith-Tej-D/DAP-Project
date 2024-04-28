from dagster import repository
from etl import *

@repository
def my_etl_repository():
    """
    Repository to hold ETL jobs for managing data extraction and ingestion.
    """
    return [downloading_datasets ,loading_dataset_1, loading_dataset_2]
