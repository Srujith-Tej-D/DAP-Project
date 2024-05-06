#imporing all the required Libraries
import pymongo
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime
from dagster import String, op, Out, job , repository , Output , In , OpExecutionContext , Failure , Tuple
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import logging
from sqlalchemy.exc import DatabaseError
from sqlalchemy import create_engine, text

# Set up a basic logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')




# Creating a Dagster OP to extract datafrom MongoDb by specifying dependency of OP's ingesting_violations_json_to_mongo & ingesting_crashes_csv_to_mongo 

@op(
    ins={"ingesting_violations_json_to_mongo": In(), "ingesting_crashes_csv_to_mongo": In()},
    out={
        "violations_df": Out(dagster_type=pd.DataFrame),
        "traffic_df": Out(dagster_type=pd.DataFrame)
    }
)

def mongo_extraction(context: OpExecutionContext,  ingesting_violations_json_to_mongo, ingesting_crashes_csv_to_mongo):
    
    mongo_connection_string = "mongodb://dap:dapsem1@localhost:27017/admin"
    start_of_2023 = datetime(2023, 1, 1)
    #Extracting Data from MongoDB only from 2023

    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client.DapDatabase
        context.log.info("Connected to MongoDB successfully.")
        logging.info("Connected to MongoDB successfully.")

        # Extract 'traffic_crashes' data from specified DATE
        traffic_crashes_data = list(db['traffic_crashes'].find({
            "CRASH_DATE": {"$gte": start_of_2023}
        }))
        context.log.info(f"Extracted {len(traffic_crashes_data)} entries from 'traffic_crashes' collection.")

        # Extract 'violations' data from specified DATE
        violations_data = list(db['violations'].find({
            "violation_date": {"$gte": start_of_2023}
        }))
        # Converting the Extracted data to pandas dataframe for further operations
        violations_df = pd.DataFrame(violations_data)
        traffic_df = pd.DataFrame(traffic_crashes_data)
        context.log.info("Successfully extracted data from Mongodb.")
        logging.info("Successfully extracted data from Mongodb.")


        # Output the dataframes
        return Output(violations_df, "violations_df"), Output(traffic_df, "traffic_df")

    except pymongo.errors.ConnectionError as e:
        context.log.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        raise




# Creating a Dagster OP for transform and load with dependency of "violations_df" & traffic_df Dataframes

@op(
    ins={
        "violations_df": In(dagster_type=pd.DataFrame),
        "traffic_df": In(dagster_type=pd.DataFrame)
    }
)
def transform_and_load(context: OpExecutionContext, violations_df: pd.DataFrame, traffic_df: pd.DataFrame):
    context.log.info("Starting the data transformation process.")
    logging.info("Starting the data transformation process.")
    
    
    
    try:
        # loadging shape file foe geo data
        shapefile_path = "geo_export_bf89c8ee-f6be-45ed-9150-6c1145a167fe.shp"
        gdf = gpd.read_file(shapefile_path)
        

        
        traffic_field_drop = [
            "CRASH_DATE_EST_I" , "LANE_CNT" , "REPORT_TYPE" , "INTERSECTION_RELATED_I" , "NOT_RIGHT_OF_WAY_I" , 
                      "DATE_POLICE_NOTIFIED" , "STREET_NO" , "STREET_DIRECTION" , "STREET_NAME" , "BEAT_OF_OCCURRENCE" , "PHOTOS_TAKEN_I",
                      "STATEMENTS_TAKEN_I" , "DOORING_I" , "WORK_ZONE_I" , "WORK_ZONE_TYPE", "_id" , "WORKERS_PRESENT_I" , "NUM_UNITS", "INJURIES_FATAL",
                      "INJURIES_INCAPACITATING" , "INJURIES_NON_INCAPACITATING" , "INJURIES_REPORTED_NOT_EVIDENT" , "EC_CONTRIBUTORY_CAUSE" ,"INJURIES_NO_INDICATION",
                      "INJURIES_UNKNOWN" , "LOCATION"
        ]

        # Drop unnecessary columns in traffic
        for column in traffic_field_drop:
            traffic_df.drop(columns=[column], inplace=True, errors='ignore')

            
        violations_fields_drop = [
            "row_id", "guid", "meta1", "created_at", "meta2", "_id" ,"updated_at", 
            "meta3", "meta4", "x_coordinate", "y_coordinate" , "location"
        ]
         
         # Drop unnecessary columns in violations
        for column in violations_fields_drop:
            violations_df.drop(columns=[column], inplace=True, errors='ignore')

        

        context.log.info("Dropped unnecessary columns.")
        logging.info("Dropped unnecessary columns.")

        # Handle integer columns
        int_traffic_df = ['POSTED_SPEED_LIMIT' , 'INJURIES_TOTAL', 'CRASH_HOUR' , 
                          'CRASH_DAY_OF_WEEK' , 'CRASH_MONTH']
        for column in int_traffic_df:
            traffic_df[column] = pd.to_numeric(traffic_df[column], errors='coerce').fillna(0).astype(int)
        
        int_violations_df = ['violations']
        for column in int_violations_df:
            violations_df[column] = pd.to_numeric(violations_df[column], errors='coerce').fillna(0).astype(int)
        context.log.info("Converted specified columns to integer.")
        logging.info("Converted specified columns to integer.")

        # Handle float columns
        for df in [ traffic_df ]:
            for col in ['LATITUDE', 'LONGITUDE']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)  # Convert directly to float and replace NaN with 0.0

        for df in [violations_df]:
            for col in ['latitude', 'longitude']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)  # Convert directly to float and replace NaN with 0.0
        
        
        context.log.info("Converted specified columns to float.")
        logging.info("Converted specified columns to float.")

        # Add an initial CRASH_PLACE column with all values set to None
        violations_df['VIOLATION_PLACE'] = None
        # Add an initial CRASH_PLACE column with all values set to None
        traffic_df['CRASH_PLACE'] = None


        # Fetching CRASH_PLACE with shape file and geo pandas
        for idx, row in traffic_df.iterrows():
            point = Point(row['LONGITUDE'], row['LATITUDE'])
            if row['LONGITUDE'] != 0.0 and row['LATITUDE'] != 0.0:
                for g_index, g_row in gdf.iterrows():
                    if g_row['geometry'].contains(point):
                        traffic_df.at[idx, 'CRASH_PLACE'] = g_row['community']  # Set the community directly
                        break
    
        context.log.info("Added new runtime field CRASH_PLACE to crahses.")
        logging.info("Added new runtime field CRASH_PLACE to crahses.")

        # Fetching VIOLATION_PLACE with shape file and geo pandas
        for idx, row in violations_df.iterrows():
            point = Point(row['longitude'], row['latitude'])
            if row['longitude'] != 0.0 and row['latitude'] != 0.0:
                for g_index, g_row in gdf.iterrows():
                    if g_row['geometry'].contains(point):
                        violations_df.at[idx, 'VIOLATION_PLACE'] = g_row['community']  # Set the community directly
                        break 
        context.log.info("Added new runtime field VIOLATION_PLACE to violations.")  
        logging.info("Added new runtime field VIOLATION_PLACE to violations.")                
        violations_df.rename(columns={col: col.upper() for col in violations_df.columns}, inplace=True)                     
        # REnamng common columns in both 
        for col_name in traffic_df.columns.intersection(violations_df.columns):
            traffic_df.rename(columns={col_name: f"CRASH_{col_name}"}, inplace=True)
            violations_df.rename(columns={col_name: f"VIOLATION_{col_name}"}, inplace=True)
        context.log.info("Renamed the common columns with prefix")    
        logging.info("Renamed the common columns with prefix")
        


        traffic_df = traffic_df.astype({
                            'CRASH_RECORD_ID' : 'string',
                            'CRASH_DATE': 'datetime64[ns]', 
                            'POSTED_SPEED_LIMIT' : int,
                            'TRAFFIC_CONTROL_DEVICE' : 'string',
                            'LIGHTING_CONDITION' : 'string',
                            'DEVICE_CONDITION' : 'string',
                            'WEATHER_CONDITION' : 'string',
                            'LIGHTING_CONDITION' : 'string',
                            'FIRST_CRASH_TYPE' : 'string',
                            'TRAFFICWAY_TYPE' : 'string',
                            'ALIGNMENT' : 'string',
                            'ROADWAY_SURFACE_COND' : 'string',
                            'ROAD_DEFECT' : 'string',
                            'CRASH_TYPE' : 'string',
                            'PRIM_CONTRIBUTORY_CAUSE' : 'string',
                            'MOST_SEVERE_INJURY' : 'string',
                            'INJURIES_TOTAL' : int ,
                            'CRASH_HOUR' : int,
                            'CRASH_DAY_OF_WEEK' : int,
                            'CRASH_MONTH' : int, 
                            'CRASH_LATITUDE': 'float64', 
                            'CRASH_LONGITUDE': 'float64',
                            'CRASH_PLACE':  'string'
                            
                        })

        violations_df = violations_df.astype({
                        'VIOLATION_DATE': 'datetime64[ns]', 
                        'ADDRESS': 'string', 
                        'VIOLATIONS': int,
                        'CAMERA_ID': 'string',
                        'VIOLATION_LATITUDE': 'float64',
                        'VIOLATION_LONGITUDE': 'float64',
                        'VIOLATION_PLACE' : 'string'
                    })
        
        #adding common filed DATE_R in both
        traffic_df['DATE_R'] = pd.to_datetime(traffic_df['CRASH_DATE']).dt.date
        violations_df['DATE_R'] = pd.to_datetime(violations_df['VIOLATION_DATE']).dt.date

        #Merging with help of common column
        traffic_df['merge_index'] = traffic_df.groupby('DATE_R').cumcount()
        violations_df['merge_index'] = violations_df.groupby('DATE_R').cumcount()

        merged_df = pd.merge(traffic_df, violations_df, on=['DATE_R', 'merge_index'], how='left')
        merged_df.drop(columns='merge_index', inplace=True)

        context.log.info("Combined dataframes into a single DataFrame.")
        logging.info("Combined dataframes into a single DataFrame.")

        engine = create_engine('postgresql://postgres:srudap@localhost:5432/dapdb')
        # Attempt to create the database if it doesn't exist
        try:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT")
                connection.execute(text("CREATE DATABASE dapdb"))
        except DatabaseError as e:
            if "already exists" in str(e):
                context.log.info("Database 'dapdb' already exists.")
            else:
                raise
        # Connect to the newly created or existing database
        engine = create_engine('postgresql://postgres:srudap@localhost:5432/postgres')
        try:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT")
                # Loading Merged Df to postgresql in datatable
                merged_df.to_sql('datatable', connection, if_exists='replace', index=False)
                context.log.info("Data Successfully loaded into postgresql DataBase.")
                logging.info("Data Successfully loaded into postgresql DataBase.")
        except Exception as e:
            context.log.error(f"An error occurred during data load: {e}")
            raise
    except Exception as e:
        context.log.error(f"An error occurred during transformation and load: {e}")
        raise    
