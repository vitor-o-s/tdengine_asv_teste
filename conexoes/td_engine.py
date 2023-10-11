import concurrent.futures
import os
import taos

from utils.utils import get_balanced_sets, load_csv_data, loading_data
import timeit 

database_name = "teste"
stable_name = "phasor"
retention_time = "4300d"
schema = "(ts TIMESTAMP, magnitude FLOAT, Angle FLOAT, frequency FLOAT)"
tags = "Id BINARY(10)"
tables = [{"name": "city01", "tag": 1},
            {"name": "city02", "tag": 2},
            {"name": "city03", "tag": 3},
            {"name": "city04", "tag": 4},
            {"name": "city05", "tag": 5},
            {"name": "city06", "tag": 6}]
BASE_DIR = "/home/dell/tcc_package/tdengine_asv_teste/data/"
ordered_tags_list = ["city01", "city02", "city03", "city04", "city05", "city06"]

create_stable_query = f" CREATE STABLE IF NOT EXISTS {stable_name} {schema} TAGS({tags}); "

create_table_query = """ CREATE TABLE IF NOT EXISTS city01 USING phasor TAGS (1)
                    IF NOT EXISTS city02 USING phasor TAGS (2)
                    IF NOT EXISTS city03 USING phasor TAGS (3)
                    IF NOT EXISTS city04 USING phasor TAGS (4)
                    IF NOT EXISTS city05 USING phasor TAGS (5)
                    IF NOT EXISTS city06 USING phasor TAGS (6);
                    """

drop_db_query = "DROP DATABASE IF EXISTS " + database_name

query_delete = "DELETE FROM phasor"

query_by_interval = """ SELECT * FROM city01 WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833' """
query_exact_time = """ SELECT * FROM city01 WHERE ts = '2012-01-03 01:00:27.233' """
query_with_avg = """ SELECT AVG(frequency) FROM city01 """
query_with_avg_by_interval = """ SELECT AVG(magnitude) FROM city01 WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833' """

def copy_files(tables: list[str], paths: list[str]):
    copy_query = "INSERT INTO \n"
    for table, path in zip(tables, paths):
        copy_query += table +" FILE '"+ path +"'\n"
    copy_query += ";"
    return copy_query

def create_slices(data, num_slices):
    """Divide data into num_slices slices."""
    avg = len(data) // num_slices
    slices = [data[i * avg: (i + 1) * avg] for i in range(num_slices)]
    slices[num_slices-1] += data[num_slices * avg:]  # Add any remaining elements to the last slice
    return slices

def write_line(conn, queries):
    cursor = conn.cursor()
    for query in queries:
        cursor.execute(query)
    cursor.close()

def parallel_insert(slices, i):
    with concurrent.futures.ThreadPoolExecutor(max_workers=i) as executor:
        conn = taos.connect(host="localhost", port=6030, user="root", password="taosdata", database=database_name)
        futures = [executor.submit(write_line, conn, data_slice) for data_slice in slices]            
        # Wait for all threads to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()
    conn = taos.connect(host="localhost", port=6030, user="root", password="taosdata", database=database_name)
    cursor = conn.cursor()
    cursor.execute(query_delete)
    cursor.close()



if __name__ == "__main__":
    # Setup        
    results = []
    conn = taos.connect(host="localhost", port=6030, user="root", password="taosdata")
    cursor = conn.cursor()
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    conn.execute("CREATE DATABASE IF NOT EXISTS "+ database_name +" KEEP "+ retention_time)
    conn.select_db(database_name)
    conn.execute(create_stable_query)
    conn.execute(create_table_query)
    
    files = ["1klines", "5klines", "10klines", "50klines"]# , "100klines", "500klines", "648klines", "1Mlines"]
    for file in files:
        # Batch test
        paths = [BASE_DIR + file + '/'+ str(i) + '_loc.csv' for i in range(1,7)]
        copy_query = copy_files(ordered_tags_list, paths)
        lambda_copy = lambda: conn.execute(copy_query)
        results.append({f"tempo_{file}_copia":timeit.timeit(lambda_copy, number=1)})

        # Query
        lambda_query_by_interval = lambda: cursor.execute(query_by_interval)
        results.append({f"tempo_{file}_query_by_interval":timeit.timeit(lambda_query_by_interval, number=1)})

        lambda_query_exact_time = lambda: cursor.execute(query_exact_time)
        results.append({f"tempo_{file}_query_exact_time":timeit.timeit(lambda_query_exact_time, number=1)})

        lambda_query_with_avg = lambda: cursor.execute(query_with_avg)
        results.append({f"tempo_{file}_query_with_avg":timeit.timeit(lambda_query_with_avg, number=1)})

        lambda_query_with_avg_by_interval = lambda: cursor.execute(query_with_avg_by_interval)
        results.append({f"tempo_{file}_query_with_avg_by_interval":timeit.timeit(lambda_query_with_avg_by_interval, number=1)})

        # Clean subtables
        cursor.execute(query_delete)

    # Parallel Ingestion
    # The same datasize from the original paper
    print('Iniciando teste de paralelismo')
    # path = BASE_DIR + '10klines/' # final_dataset.csv' #648Klines
    lines = []
    for i in range(1,7):
        data = load_csv_data(BASE_DIR + '10klines/' + f'{i}_loc.csv') #648Klines
        # print(data[0])
        data = [(row[0].replace("'", ""), *row[1:]) for row in data]
        formated_lines = [f"INSERT INTO city0{str(i)} USING phasor TAGS({str(i)}) VALUES {x};" for x in data]
        lines += formated_lines
    n_threads = [1, 2, 4] # , 8, 16, 32]
    for i in n_threads:
        print("N. THREADS:", i)        
        slices = create_slices(lines, i)
        elapsed_time = timeit.timeit(lambda: parallel_insert(slices, i), number=1)
        results.append({f"tempo_{i}_threads": elapsed_time})
        print(f"Time taken with {i} threads: {elapsed_time:.2f} seconds")

    # Teardown  
    print("Excuting the following query:\n",drop_db_query)
    conn.execute(drop_db_query)
    print('Close connection!')
    cursor.close()
    conn.close()

    print(results)
