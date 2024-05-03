from dagster import op, fs_io_manager ,build_op_context , OpExecutionContext
import requests
import os

import requests
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def file_download(url, destination_path):
    if os.path.exists(destination_path):
        logging.info("File exists going to next step")
        return True
    else:
<<<<<<< Updated upstream
        logging.error(f"Failed to download the file from data catlog {url}")
        return False

=======
        try:
            with open(destination_path, "wb") as file:
                response = requests.get(url)
                file.write(response.content)
            return True
        except Exception as e:
            logging.error(f"Failed to download file from {url}: {e}")
            return False
>>>>>>> Stashed changes
@op
def saving_datasets(context : OpExecutionContext):
    json_url = "https://data.cityofchicago.org/api/views/hhkd-xvj4/rows.json?accessType=DOWNLOAD"
    json_destination_path = "violations.json"
    logging.info("Started Downloading violations.json")
    context.log.info("Started Downloading violations.json")
    csv_url = "https://data.cityofchicago.org/api/views/85ca-t3if/rows.csv?accessType=DOWNLOAD"
    csv_destination_path = "crashes.csv"
    logging.info("Started Downloading crashes.csv")
    context.log.info("Started Downloading crashes.csv")

    if file_download(json_url, json_destination_path):
        logging.info("Downloaded violations.json")
        context.log.info("Downloaded violations.json")
        logging.info("Downloaded crashes.csv")
        context.log.info("Downloaded crashes.csv")
        logging.info("JSON file saved as: {}".format(json_destination_path))
        context.log.info("JSON file saved as: {}".format(json_destination_path))
    else:
        logging.error("JSON file download failed.")
    
    if file_download(csv_url, csv_destination_path):
        logging.info("CSV file saved as: {}".format(csv_destination_path))
        context.log.info("CSV file saved as: {}".format(csv_destination_path))
    else:
        logging.error("CSV file download failed.")
