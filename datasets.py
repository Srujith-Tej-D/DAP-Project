from dagster import op, fs_io_manager ,build_op_context
import requests


import requests
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def file_download(url: str, destination_path: str) -> bool:
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(destination_path, 'wb') as f:
            f.write(response.content)
        return True
    else:
        logging.error(f"Failed to download the file from {url}")
        return False

@op
def saving_datasets():
    json_url = "https://data.cityofchicago.org/api/views/hhkd-xvj4/rows.json?accessType=DOWNLOAD"
    json_destination_path = "violations.json"
    csv_url = "https://data.cityofchicago.org/api/views/85ca-t3if/rows.csv?accessType=DOWNLOAD"
    csv_destination_path = "crashes.csv"

    if file_download(json_url, json_destination_path):
        logging.info("JSON file saved as: {}".format(json_destination_path))
    else:
        logging.error("JSON file download failed.")
    
    if file_download(csv_url, csv_destination_path):
        logging.info("CSV file saved as: {}".format(csv_destination_path))
    else:
        logging.error("CSV file download failed.")
