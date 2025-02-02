# Updated Producer Script
import csv
from google.cloud import pubsub_v1
import glob
import json
import os

# Set the Google Cloud credentials
gcp_credential_files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credential_files[0]

# Set your project ID and topic name
project_id = "velvety-study-448822-h6"
topic_name = "LabelsTopic"  

# Create a publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

# Read the CSV file and publish 
csv_file = "Labels.csv"  
with open(csv_file, mode="r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Serialize each row as a JSON string
        message = json.dumps(row).encode("utf-8")
        print(f"Producing record: {message}")
        # Publish the message
        future = publisher.publish(topic_path, message)
        future.result()  

print("All messages have been published.")
