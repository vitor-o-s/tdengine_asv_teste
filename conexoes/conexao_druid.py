from pydruid.db import connect
import timeit
import concurrent.futures
import json
import requests
import os
from utils.utils import load_csv_data, loading_data  # Assuming these functions are defined in your utils

BASE_DIR = "/home/dell/tcc_package/tdengine_asv_teste/data/"
DRUID_BROKER_URL = 'druid://localhost:8082/druid/v2/sql/'
SCHEMA = ("ts", "magnitude", "angle", "frequency", "location")

# Druid does not support table creation or deletion via SQL in the same way as relational databases.
# You might need to define your data schema and ingestion spec separately.

# Queries might need to be adapted based on your Druid data schema
query_by_interval = "SELECT * FROM phasor WHERE __time BETWEEN TIMESTAMP '2012-01-03T01:00:24Z' AND TIMESTAMP '2012-01-03T01:02:24Z'"
query_exact_time = "SELECT * FROM phasor WHERE __time = TIMESTAMP '2012-01-03T01:00:27Z'"
query_with_avg = "SELECT AVG(frequency) FROM phasor"
query_with_avg_by_interval = "SELECT AVG(magnitude) FROM phasor WHERE __time BETWEEN TIMESTAMP '2012-01-03T01:00:24Z' AND TIMESTAMP '2012-01-03T01:02:24Z'"

def submit_ingestion_task(ingestion_spec_json, file_path, max_num_concurrent_sub_tasks=None):
    # Update the file path in the ingestion spec
    ingestion_spec_json['spec']['ioConfig']['inputSource']['baseDir'] = 'data/' + file_path
    
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
    else:
        print("Failed to submit ingestion task!")
        print("Status Code:", response.status_code)
        print("Error Response:", response.text)


if __name__ == "__main__":
    conn = connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http')
    cursor = conn.cursor()
    results = []
    files = ["1klines", "5klines", "10klines", "50klines"]
    # Load the ingestion spec from the JSON file
    with open('/home/dell/tcc_package/tdengine_asv_teste/utils/ingestion_spec.json', 'r') as file:
        ingestion_spec = json.load(file)

    for file in files:

        # Batch
        file_path = BASE_DIR + file + '/final_dataset.csv'
        
        # If using 'index_parallel', specify the maximum number of concurrent sub-tasks
        max_num_concurrent_sub_tasks = None  # Adjust as needed
        
        # Submit the ingestion task
        results.append({f"tempo_{file}_copia":
                        submit_ingestion_task(ingestion_spec, file, max_num_concurrent_sub_tasks)
                        })


        # Query
        # lambda_query_by_interval = lambda: cursor.execute(query_by_interval)
        # results.append({f"tempo_{file}_query_by_interval":timeit.timeit(lambda_query_by_interval, number=1)})

        # lambda_query_exact_time = lambda: cursor.execute(query_exact_time)
        # results.append({f"tempo_{file}_query_exact_time":timeit.timeit(lambda_query_exact_time, number=1)})

        # lambda_query_with_avg = lambda: cursor.execute(query_with_avg)
        # results.append({f"tempo_{file}_query_with_avg":timeit.timeit(lambda_query_with_avg, number=1)})

        # lambda_query_with_avg_by_interval = lambda: cursor.execute(query_with_avg_by_interval)
        # results.append({f"tempo_{file}_query_with_avg_by_interval":timeit.timeit(lambda_query_with_avg_by_interval, number=1)})

        # End of tests
        # cursor.execute(query_delete)
       

# if __name__ == "__main__":
    


    # Parallel Querying
    # print('Starting parallel query test')
    # file_path = BASE_DIR + '10klines/final_dataset.csv'
    # data = load_csv_data(file_path)
    # n_threads = [1, 2, 4]
    # for i in n_threads:
    #    print("N. THREADS:", i)
    #    elapsed_time = timeit.timeit(lambda: parallel_query(data, i), number=1)
    #    results.append({f"tempo_{i}_threads": elapsed_time})
    #    print(f"Time taken with {i} threads: {elapsed_time:.2f} seconds")

    # print(results)
