#imporing all the required Libraries
from dagster import op, Out , OpExecutionContext
from datetime import datetime
import pymongo
from pymongo import errors
import json
import logging
import csv
from datasets import *

# Set up a basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



@op(out=Out(bool))  #Extracting Csv data from file and ingesting it to Mongo Db
def ingesting_crashes_csv_to_mongo(context : OpExecutionContext, saving_datasets) -> bool:
    mongo_connection_string = "mongodb://dap:dapsem1@localhost:27017/admin"
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["DapDatabase"]

    traffic_crashes_path = r"crashes.csv"
    collection_name = "traffic_crashes"  # Our MongoDb collection Name for Crashes

    try:
        with open(traffic_crashes_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            all_data = []

            for row in reader:
                # Parsing CRASH_DATE and DATE_POLICE_NOTIFIED to datetime columns
                if 'CRASH_DATE' in row:
                    row['CRASH_DATE'] = datetime.strptime(row['CRASH_DATE'], '%m/%d/%Y %I:%M:%S %p')
                if 'DATE_POLICE_NOTIFIED' in row:
                    row['DATE_POLICE_NOTIFIED'] = datetime.strptime(row['DATE_POLICE_NOTIFIED'], '%m/%d/%Y %I:%M:%S %p')
                if 'POSTED_SPEED_LIMIT' in row:
                    row['POSTED_SPEED_LIMIT'] = int(row['POSTED_SPEED_LIMIT'])
                all_data.append(row)

            db[collection_name].insert_many(all_data)


        logging.info("Violations Json Ingestion successfull")
        context.log.info("Violations Json Ingestion successfull")
        result = True

    except Exception as e:
        logging.error("An error occurred: %s", e)
        result = False

    return result



@op(out=Out(bool)) #Extracting Csv data from file and ingesting it to Mongo Db
def ingesting_violations_json_to_mongo(context : OpExecutionContext,saving_datasets) -> bool:
    mongo_connection_string = "mongodb://dap:dapsem1@localhost:27017/admin"
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["DapDatabase"]
    collection = db['violations']  # Our MongoDb collection Name for violations

    file_path = r"violations.json"  

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
            data_dict["_id"] = data_dict["row_id"]
            try:
                # Insert data into MongoDB
                collection.insert_one(data_dict)
                
            except errors.DuplicateKeyError as dke:
                logging.error("Duplicate Key Error: %s", dke)
                continue
        logging.info("Violations Json Ingestion successfull")
        context.log.info("Violations Json Ingestion successfull")
        
        result = True


    except Exception as e:
        logging.error("An error occurred: %s", e)
        result = False

    return result
