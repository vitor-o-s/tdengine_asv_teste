import psycopg2
from pgcopy import CopyManager

BASE_DIR = "/home/dell/tcc_package/tdengine_asv_teste/data/"
CONNECTION = "postgres://postgres:password@127.0.0.1:5432/postgres"
SCHEMA = ["ts", "magnitude", "angle", "frequency"]

query_create_phasor_hypertable = """
                                CREATE TABLE phasor (
                                    ts TIMESTAMPTZ NOT NULL,
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

query_by_interval = "SELECT * FROM city01 WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833'"
query_exact_time = "SELECT * FROM city01 WHERE ts = '2012-01-03 01:00:27.233'"
query_with_avg = "SELECT AVG(frequency) FROM city01 "
query_with_avg_by_interval = "SELECT AVG(magnitude) FROM city01 WHERE ts BETWEEN '2012-01-03 01:00:24.000' AND '2012-01-03 01:02:24.833'"


file_path = BASE_DIR + "1klines/final_dataset.csv"


def teardown(cursor):
    print("Cleaning the DB")
    cursor.execute("DROP TABLE phasor;")
    print("Done! Clossing cursor and ending application!")


if __name__ == "__main__":
    with psycopg2.connect(CONNECTION) as conn:
        # Setup
        cursor = conn.cursor()
        cursor.execute(query_create_phasor_hypertable)

        # Parallel test

        # Batch test
        mgr = CopyManager(conn, "phasor", SCHEMA)
        mgr.copy(file_path)
        # cursor.execute(f"""COPY phasor FROM '{file_path}' DELIMITER ',' CSV HEADER;""")
        # Query

        # Compression Test

        # Validation
        cursor.execute("SELECT * FROM phasor")
        for row in cursor.fetchall():
            print(row)

        # End of tests
        teardown(cursor=cursor)
        cursor.close()
