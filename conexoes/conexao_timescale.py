import psycopg2
import timeit
import time
import concurrent.futures
from pgcopy import CopyManager
from utils.utils import load_csv_data, loading_data

BASE_DIR = "/home/dell/tcc_package/tdengine_asv_teste/data/"
CONNECTION = "postgres://postgres:password@127.0.0.1:5432/postgres"
SCHEMA = ("ts", "magnitude", "angle", "frequency", "location")

query_create_phasor_hypertable = """
                                DROP TABLE IF EXISTS phasor;
                                CREATE TABLE IF NOT EXISTS phasor (
                                    ts TIMESTAMP NOT NULL,
                                    magnitude FLOAT,
                                    angle FLOAT, 
                                    frequency FLOAT,
                                    location INT
                                );
                                SELECT create_hypertable('phasor', 'ts');
                                """
query_hypertable_size = """
                        SELECT 
                            hypertable_name, 
                            pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name))) 
                        FROM 
                            timescaledb_information.hypertables;
                        """

query_compression_table = """
                        ALTER TABLE phasor SET (
                            timescaledb.compress,
                            timescaledb.compress_segmentby = 'location'
                        );
                        """
query_compression_policy = "SELECT add_compression_policy('phasor', INTERVAL '7 days');"

query_delete = "DELETE FROM phasor"

query_by_interval = "SELECT * FROM phasor WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833'"
query_exact_time = "SELECT * FROM phasor WHERE ts = '2012-01-03 01:00:27.233'"
query_with_avg = "SELECT AVG(frequency) FROM phasor "
query_with_avg_by_interval = "SELECT AVG(magnitude) FROM phasor WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833'"


file_path = BASE_DIR + "1klines/final_dataset.csv"

 
def teardown(cursor):
    print("Cleaning the DB")
    cursor.execute("DROP TABLE phasor;")
    print("Done! Clossing cursor and ending application!")

def write_line(conn, queries):
    cursor = conn.cursor()
    for query in queries:
        cursor.execute(query)
    cursor.close()

def create_slices(data, num_slices):
    """Divide data into num_slices slices."""
    avg = len(data) // num_slices
    slices = [data[i * avg: (i + 1) * avg] for i in range(num_slices)]
    slices[num_slices-1] += data[num_slices * avg:]  # Add any remaining elements to the last slice
    return slices

def parallel_insert(data, i):
    formated_lines = [f"INSERT INTO phasor(ts, magnitude, angle, frequency, location) VALUES {x};" for x in data]
    slices = create_slices(formated_lines, i)
    with concurrent.futures.ThreadPoolExecutor(max_workers=i) as executor:
        futures = [executor.submit(write_line, psycopg2.connect(CONNECTION), data_slice) for data_slice in slices]            
        # Wait for all threads to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == "__main__":
    with psycopg2.connect(CONNECTION) as conn:
        # Setup
        results = []
        cursor = conn.cursor()
        cursor.execute(query_create_phasor_hypertable)
        mgr = CopyManager(conn, "phasor", SCHEMA)
        files = ["1klines", "5klines", "10klines", "50klines", "100klines", "500klines", "648klines", "1Mlines"]
        for file in files:
            data = loading_data(BASE_DIR + file + "/final_dataset.csv")

            # Batch test
            lambda_copy = lambda: mgr.copy(data)
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

            # End of tests
            cursor.execute(query_delete)
        cursor.close()

    # Parallel Ingestion
    # The same datasize from the original paper
    print('Iniciando teste de paralelismo')
    file_path = BASE_DIR + '100klines/final_dataset.csv' #648Klines
    data = load_csv_data(file_path)
    n_threads = [1, 2, 4, 8, 16, 32]
    for i in n_threads:
        print("N. THREADS:", i)
        elapsed_time = timeit.timeit(lambda: parallel_insert(data, i), number=1)
        results.append({f"tempo_{i}_threads": elapsed_time})
        print(f"Time taken with {i} threads: {elapsed_time:.2f} seconds")
        with psycopg2.connect(CONNECTION) as conn:
            cursor = conn.cursor()
            cursor.execute(query_delete)
            cursor.close()

    # Teardown
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        teardown(cursor=cursor)
        cursor.close()
    print(results)