import psycopg2
import timeit
import psutil
import os
from pgcopy import CopyManager
from utils.utils import get_balanced_sets, loading_data

BASE_DIR = "/home/dell/tcc_package/tdengine_asv_teste/data/"
CONNECTION = "postgres://postgres:password@127.0.0.1:5432/postgres"
SCHEMA = ("ts", "magnitude", "angle", "frequency", "location")

query_create_phasor_hypertable = """
                                CREATE TABLE phasor (
                                    ts TIMESTAMP NOT NULL,
                                    magnitude FLOAT,
                                    angle FLOAT, 
                                    frequency FLOAT,
                                    location INT
                                );
                                SELECT create_hypertable('phasor', 'ts');
                                --ALTER TABLE phasor SET (
                                --    timescaledb.compress,
                                --    timescaledb.compress_segmentby = 'location'
                                --);
                                --SELECT add_compression_policy('phasor', INTERVAL '7 days');
                                """
query_hypertable_size = """
                        SELECT 
                            hypertable_name, 
                            pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name))) 
                        FROM 
                            timescaledb_information.hypertables;
                        """

query_by_interval = "SELECT * FROM phasor WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833'"
query_exact_time = "SELECT * FROM phasor WHERE ts = '2012-01-03 01:00:27.233'"
query_with_avg = "SELECT AVG(frequency) FROM phasor "
query_with_avg_by_interval = "SELECT AVG(magnitude) FROM phasor WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833'"


file_path = BASE_DIR + "1klines/final_dataset.csv"

 
def teardown(cursor):
    print("Cleaning the DB")
    cursor.execute("DROP TABLE phasor;")
    print("Done! Clossing cursor and ending application!")

def measure_peak_memory_usage(func, *args, **kwargs):
    process = psutil.Process(os.getpid())
    baseline_memory = process.memory_info().rss
    func(*args, **kwargs)
    peak_memory = process.memory_info().rss
    print('Peak:', peak_memory)
    print('Base:', baseline_memory)
    memory_usage = (peak_memory - baseline_memory) / (1024 * 1024)
    return memory_usage

if __name__ == "__main__":
    with psycopg2.connect(CONNECTION) as conn:
        # Setup
        results = []
        cursor = conn.cursor()
        cursor.execute(query_create_phasor_hypertable)
        mgr = CopyManager(conn, "phasor", SCHEMA)
        data = loading_data(file_path)

        # Parallel test

        # Batch test
        lambda_copy = lambda: mgr.copy(data)
        results.append({"tempo_copia":timeit.timeit(lambda_copy, number=1)})

        # Query
        lambda_query_by_interval = lambda: cursor.execute(query_by_interval)
        results.append({"tempo_query_by_interval":timeit.timeit(lambda_query_by_interval, number=1)})
        results.append({"pico_memoria_query_by_interval":measure_peak_memory_usage(lambda_query_by_interval)})

        lambda_query_exact_time = lambda: cursor.execute(query_exact_time)
        results.append({"tempo_query_exact_time":timeit.timeit(lambda_query_exact_time, number=1)})

        lambda_query_with_avg = lambda: cursor.execute(query_with_avg)
        results.append({"tempo_query_with_avg":timeit.timeit(lambda_query_with_avg, number=1)})

        lambda_query_with_avg_by_interval = lambda: cursor.execute(query_with_avg_by_interval)
        results.append({"tempo_query_with_avg_by_interval":timeit.timeit(lambda_query_with_avg_by_interval, number=1)})

        # Compression Test
        cursor.execute(query_hypertable_size)
        for row in cursor.fetchall():
            print(row)        
        
        # End of tests
        teardown(cursor=cursor)
        cursor.close()
    print(results)