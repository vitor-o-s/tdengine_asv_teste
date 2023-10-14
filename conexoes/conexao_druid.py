from pydruid.db import connect
import timeit
# import concurrent.futures
import json
import requests
import time
# import os
# from utils.utils import load_csv_data, loading_data  # Assuming these functions are defined in your utils

BASE_DIR = "/home/dell/tcc_package/tdengine_asv_teste/data/"
DRUID_BROKER_URL = 'druid://localhost:8082/druid/v2/sql/'
# SCHEMA = ("ts", "magnitude", "angle", "frequency", "location")

# Queries might need to be adapted based on your Druid data schema
query_by_interval = "SELECT * FROM phasor WHERE __time BETWEEN TIMESTAMP '2012-01-03 01:00:24.000' AND '2012-01-03 02:00:24.000'"
query_exact_time = "SELECT * FROM phasor WHERE __time = TIMESTAMP '2012-01-03 01:00:27.000'"
query_with_avg = "SELECT AVG(frequency) FROM phasor"
query_with_avg_by_interval = "SELECT AVG(magnitude) FROM phasor WHERE __time BETWEEN TIMESTAMP '2012-01-03 01:00:24.000' AND '2012-01-03 02:00:24.000'"

def submit_ingestion_task(ingestion_spec_json, file_path, max_num_concurrent_sub_tasks=None):
    # Update the file path in the ingestion spec
    # ingestion_spec_json['spec']['ioConfig']['inputSource']['baseDir'] = 'data/' + file_path
    ingestion_spec_json['spec']['ioConfig']['inputSource']['files'] = ['data/' + file_path + '/final_dataset.csv']
    
    # If task type is parallel, set the number of concurrent sub-tasks
    if max_num_concurrent_sub_tasks is not None:
        ingestion_spec_json['spec']['tuningConfig']['maxNumConcurrentSubTasks'] = max_num_concurrent_sub_tasks
    
    # Convert the spec to JSON format
    ingestion_task_json = json.dumps(ingestion_spec_json)

    # Submit the task to Druid overlord
    druid_overlord_url = 'http://localhost:8081/druid/indexer/v1/task'
    # lambda_batch = requests.post(druid_overlord_url, data=ingestion_task_json, headers={'Content-Type': 'application/json'})
    # return timeit.timeit(lambda_batch, number=1)
    response = requests.post(druid_overlord_url, data=ingestion_task_json, headers={'Content-Type': 'application/json'})

    # Check the response
    if response.status_code == 200:
        print("Ingestion task submitted successfully!")
        print("Task ID:", response.json()['task'])
        return response.json()['task']
    else:
        print("Failed to submit ingestion task!")
        print("Status Code:", response.status_code)
        print("Error Response:", response.text)


if __name__ == "__main__":
    conn = connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http')
    cursor = conn.cursor()
    results = []
    files = ["1klines", "5klines"]# , "10klines", "50klines", "100klines", "500klines", "648klines", "1Mlines"]
    # Load the ingestion spec from the JSON file
    with open('/home/dell/tcc_package/tdengine_asv_teste/utils/ingestion_spec.json', 'r') as file:
        ingestion_spec = json.load(file)

    for file in files:

        # Batch
        file_path = BASE_DIR + file + '/final_dataset.csv'
        
        # If using 'index_parallel', specify the maximum number of concurrent sub-tasks
        max_num_concurrent_sub_tasks = None  # Adjust as needed
        
        # Submit the ingestion task
        task_id = submit_ingestion_task(ingestion_spec, file, max_num_concurrent_sub_tasks)
        
        time.sleep(3)
        response = requests.get('http://localhost:8081/druid/indexer/v1/task/'+task_id+'/status')
        while response.json()['status']['status'] != 'SUCCESS':
            time.sleep(3)
            response = requests.get('http://localhost:8081/druid/indexer/v1/task/'+task_id+'/status')
        results.append({f"tempo_{file}_copia": response.json()['status']['duration']/1000})
        time.sleep(3)

        # Query
        cursor.execute(query_by_interval)
        lambda_query_by_interval = lambda: cursor.execute(query_by_interval)
        results.append({f"tempo_{file}_query_by_interval":timeit.timeit(lambda_query_by_interval, number=1)})

        lambda_query_exact_time = lambda: cursor.execute(query_exact_time)
        results.append({f"tempo_{file}_query_exact_time":timeit.timeit(lambda_query_exact_time, number=1)})

        lambda_query_with_avg = lambda: cursor.execute(query_with_avg)
        results.append({f"tempo_{file}_query_with_avg":timeit.timeit(lambda_query_with_avg, number=1)})

        lambda_query_with_avg_by_interval = lambda: cursor.execute(query_with_avg_by_interval)
        results.append({f"tempo_{file}_query_with_avg_by_interval":timeit.timeit(lambda_query_with_avg_by_interval, number=1)})

        # End of tests
        # 2 requests, mark data as unused and after delete
        # Mark Unused
        unused_url = 'http://localhost:8081/druid/coordinator/v1/datasources/phasor/markUnused'
        unused_json = json.dumps({"interval": "1000-01-01/2023-10-13"})
        response = requests.post(unused_url, data=unused_json, headers={'Content-Type': 'application/json'})
        time.sleep(3)
        # Kill data
        # kill_url = 'http://localhost:8081/druid/indexer/v1/task'
        # kill_json = json.dumps({})
        interval = "1000-01-01/2023-10-13"
        # kill_url = f'http://localhost:8081/druid/coordinator/v1/datasources/phasor/intervals/{interval}'
        # response = requests.delete(kill_url)
        # # Check the response
        # if response.status_code == 200:
        #    print("Ingestion task submitted successfully!")
        #    print("Task ID:", response.json()['task'])
        #    # return response.json()['task']
        # else:
        #     print("Failed to submit ingestion task!")
        #     print("Status Code:", response.status_code)
        #     print("Error Response:", response.text)
        
        task_url = "http://localhost:8081/druid/indexer/v1/task"
        headers = {'Content-Type': 'application/json'}
        
        # Construct the task payload
        task_payload = {
            "type": "kill",
            "id": f"kill_phasor_{interval.replace('/', '_')}",
            "dataSource": 'phasor',
            "interval": interval
        }
        
        # Convert the task payload to JSON format
        task_payload_json = json.dumps(task_payload)
        
        response = requests.post(task_url, data=task_payload_json, headers=headers)
        
        if response.status_code == 200:
            print("Kill task submitted successfully!")
            print("Task ID:", response.json()['task'])
        else:
            print("Failed to submit kill task!")
            print("Status Code:", response.status_code)
            print("Error Response:", response.text)

        time.sleep(5)
        print('Seguindo')

        # response = requests.post(kill_url, data=kill_json, headers={'Content-Type': 'application/json'})
        # Wait until success

    # Parallel Querying
    print('Starting parallel query test')
    n_threads = [1, 2, 4]
    thread_task_id = []
    for i in n_threads:
       print("N. THREADS:", i)
       task_id = submit_ingestion_task(ingestion_spec, '10klines', i)
       time.sleep(3)
       response = requests.get('http://localhost:8081/druid/indexer/v1/task/'+task_id+'/status')
       while response.json()['status']['status'] != 'SUCCESS':
          time.sleep(3)
          response = requests.get('http://localhost:8081/druid/indexer/v1/task/'+task_id+'/status')
       results.append({f"tempo_{i}_threads": response.json()['status']['duration']/1000})

    # make a request from ids to get the time
    # Clean datasource
    # 2 requests, mark data as unused and after delete
    print(results)
