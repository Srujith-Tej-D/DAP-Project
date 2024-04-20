from dagster import job
from extract import *

@job
def jobs():
    extract_and_store_violations_in_mongo()