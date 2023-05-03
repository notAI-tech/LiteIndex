import requests
import json
import time
import random

API_URL = "http://localhost:8000/add_predictions_for_an_app"  # Replace this with the actual URL of the API.

def send_request():
    processing_time_sum = 0
    pipelines = [
        {"name": "tiling", "type": "Tiling"},
        {"name": "translation", "type": "Translation"},
        {"name": "abstractive_summarization", "type": "Abstractive Summarization"},
        {"name": "extractive_summarization", "type": "Extractive Summarization"},
    ]

    for pipeline in pipelines:
        pipeline["input"] = "input_placeholder"
        pipeline["output"] = "output_placeholder"
        pipeline_time = random.randint(1, 10)
        pipeline["processing_time"] = pipeline_time
        pipeline["success"] = random.choice([True, False])
        processing_time_sum += pipeline_time

    total_processing_time = processing_time_sum + random.randint(1, 2)
    created_at = time.time()

    data = {
        "unique_id": f"analytics_{int(created_at)}",
        "app_name": "analytics",
        "app_version": "1.0",
        "pipelines": pipelines,
        "processing_time": total_processing_time,
        "created_at": created_at,
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(API_URL, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        print("Request sent successfully.")
    else:
        print(f"Error sending request. Status code: {response.status_code}")

while True:
    send_request()
    time.sleep(5)
