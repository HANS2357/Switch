import csv
from elasticsearch import Elasticsearch
import time
from Custom_Logger import logger
# Elasticsearch connection settings
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Path to your CSV file
csv_file_path = 'metrics.csv'

# Function to send data to Elasticsearch
index_name = 'final_metrics_data'
#final_metrics_data

def send_to_elasticsearch(data):
    try:
        print(data)
        # Send the data to Elasticsearch
        es.index(index=index_name, body=data)
    except Exception as e:
        str = f"Failed to upload metrics data to ES: {str(e)}"
        logger.error(str)
        print(str)
    

# Function to process the CSV file
def process_csv_file():

    try:
        with open(csv_file_path, 'r') as file:

            if file.read().strip() == '':
                # print("File is empty. Skipping processing.")
                return
            file.seek(0)
            reader = csv.reader(file)
            # next(reader)  # Skip the header row

            for row in reader:
                # Extract the relevant data from the CSV row
                ts = row[0]
                log_id = int(row[1])
                confidence = float(row[2])
                model_name = row[3]
                cpu = float(row[4])
                detection_boxes = int(row[5])
                model_processing_time = float(row[6])
                image_processing_time = float(row[7])
                absolute_time_from_start = float(row[8])
                utility = float(row[9])
                carbon_emission = float(row[10])
                carbon_emission_predicted = float(row[11])
                trees = int(row[12])
                carbon_emission_yolo5n = float(row[13])
                carbon_emission_yolo5s = float(row[14]) 
                carbon_emission_yolo5m = float(row[15])
                carbon_emission_yolo5l = float(row[16])
                carbon_emission_yolo5x = float(row[17])

                # Extract other fields as needed

                # Create a dictionary to represent the log entry
                log_data = {
                    'timestamp' : ts,
                    'log_id': log_id,
                    'confidence': confidence,
                    'model_name': model_name,
                    'cpu': cpu,
                    'detection_boxes': detection_boxes,
                    'model_processing_time' : model_processing_time,
                    'image_processing_time' : image_processing_time,
                    'absolute_time_from_start': absolute_time_from_start,
                    'utility': utility,
                    'carbon_emission': carbon_emission,
                    'carbon_emission_predicted':carbon_emission_predicted,
                    'trees': trees,
                    'carbon_emission_yolo5n': carbon_emission_yolo5n,
                    'carbon_emission_yolo5s': carbon_emission_yolo5s,
                    'carbon_emission_yolo5m': carbon_emission_yolo5m,
                    'carbon_emission_yolo5l': carbon_emission_yolo5l,
                    'carbon_emission_yolo5x': carbon_emission_yolo5x
                    # Add other fields as needed
                }

                # Send the log data to Elasticsearch
                # file.truncate(0)
                send_to_elasticsearch(log_data)

            with open(csv_file_path, 'r+') as file:
            # Read the file content
            # Truncate the file to remove existing data
                file.truncate(0)
    except Exception as e:
        str = f"Failed to process metrics file thaht was to be uploaded to ES: {str(e)}"
        logger.error(str)
        print(str)

if not es.indices.exists(index=index_name):
        # Create the Elasticsearch index with mapping
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "log_id": {"type": "integer"},
                "confidence": {"type": "float"},
                "model_name": {"type": "keyword"},
                "cpu": {"type": "float"},
                "detection_boxes": {"type": "integer"},
                "model_processing_time": {"type": "float"},
                "image_processing_time": {"type": "float"},
                "absolute_time_from_start": {"type": "float"},
                "utility": {"type": "float"},
                "carbon_emission": {"type": "float"},
                "carbon_emission_predicted":{"type":"float"},
                "trees":{"type":"int"},
                "carbon_emission_yolo5n": {"type": "float"},
                "carbon_emission_yolo5s": {"type": "float"},
                "carbon_emission_yolo5m": {"type": "float"},
                "carbon_emission_yolo5l": {"type": "float"},
                "carbon_emission_yolo5x": {"type": "float"},

            }
        }
    }

    es.indices.create(index=index_name, body=mapping)

es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})

process_csv_file()

# Continuously monitor the CSV file for updates
while True:
    time.sleep(1)
    process_csv_file()