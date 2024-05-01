from dagster import op, Out
from datetime import datetime
import pymongo
from pymongo import errors
import json
import logging
import csv

# Set up a basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

@op(out=Out(bool))
def ingesting_violations_json_to_mongo() -> bool:
    mongo_connection_string = "mongodb://dap:dapsem1@localhost:27017/admin"
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["DapDatabase"]
    collection = db['violations']

    file_path = r"violations.json"  # Use double backslashes for Windows paths

    try:
        # Load JSON data from file
        with open(file_path, 'r') as file:
            full_data = json.load(file)

        data_entries = full_data['data']
        columns = ['row_id', 'guid', 'meta1', 'created_at', 'meta2', 'updated_at', 'meta3', 'meta4', 'address', 'camera_id', 'violation_date', 'violations', 'x_coordinate', 'y_coordinate', 'latitude', 'longitude', 'location']
        
        # Transform data into dictionaries expected by MongoDB
        data_dicts = [dict(zip(columns, entry)) for entry in data_entries]

        for data_dict in data_dicts:
            # Use row_id as the unique identifier for MongoDB documents
            data_dict['violation_date'] = datetime.strptime(data_dict['violation_date'], "%Y-%m-%dT%H:%M:%S")
            #data_dict['violations'] = int(data_dict['violations'])
            data_dict["_id"] = data_dict["row_id"]
            try:
                # Insert data into MongoDB
                collection.insert_one(data_dict)
            except errors.DuplicateKeyError as dke:
                logger.error("Duplicate Key Error: %s", dke)
                continue

        logger.info("Data successfully loaded and inserted into MongoDB.")
        result = True

    except Exception as e:
        logger.error("An error occurred: %s", e)
        result = False

    return result