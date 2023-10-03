import concurrent.futures
import os
import taos

from utils.utils import get_balanced_sets
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


class TDengine():
    
    def __init__(
            self,
            database: str,
            retention_time: str,
            stable_name: str, 
            schema: str, 
            tags: str,
            tables: list,
            host: str = "localhost",
            port: int = 6030,
            user: str = "root",
            password: str = "taosdata", 
            **kwargs
            ) -> None:
        self.database = database
        self.retention_time = retention_time
        self.stable_name = stable_name
        self.schema = schema
        self.tags = tags
        self.tables = tables
        self.conn = taos.connect(host=host,
                                 port=port,
                                 user=user,
                                 password=password)
    
        print('client info:', self.conn.client_info)
        print('server info:', self.conn.server_info)
        self.conn.execute("CREATE DATABASE IF NOT EXISTS "+ self.database+" KEEP "+ self.retention_time)
        self.conn.select_db(self.database)
    
    def create_stable(self):
        create_stable_query = f"""
        CREATE STABLE IF NOT EXISTS {self.stable_name}
        {self.schema} 
        TAGS({self.tags});
        """
        self.conn.execute(create_stable_query)
  
    def create_tables(self):
        for item in self.tables:
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {item['name']} 
                USING {self.stable_name} 
                TAGS ({str(item['tag'])});
                """
            self.conn.execute(create_table_query)
    
    def copy_files(self, tables: list[str], paths: list[str]):
        """
        """
        file_query = "INSERT INTO \n"
        for table, path in zip(tables, paths):
            file_query += table +" FILE '"+ path +"'\n"
        file_query += ";"
        print(file_query)
        self.conn.execute(file_query)

    def insert_row(self, table: str, value: str):
        self.conn.execute("INSERT INTO "+ table +" VALUES ()"+ value +");")

    def close_conn(self):
        print('Close connection!')
        self.conn.close()
    
    def drop_database(self):
        drop_db_query = "DROP DATABASE IF EXISTS "+ self.database
        print("Excuting the following query:\n",drop_db_query)
        self.conn.execute(drop_db_query)

    def query(self, query: str):
        result: taos.TaosResult = self.conn.query(query)
        print('FIELD COUNT:', result.field_count)
        print('ROW_COUNT:', result.row_count)
        rows = result.fetch_all_into_dict()
        print('ROW_COUNT:', result.row_count)
        return

    def interval_query(self, table_name: str, start_time: str, end_time: str):
        """
        Query data from a table within a specified time interval.
        """
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE ts BETWEEN '{start_time}' AND '{end_time}'
        """
        result: taos.TaosResult = self.conn.query(query)

    def exact_query(self, table_name: str, timestamp: str):
        """
        Query data from a table for a specific timestamp.
        """
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE ts = '{timestamp}'
        """
        result: taos.TaosResult = self.conn.query(query)

    def aggregation_query(self, table_name: str, aggregation_function: str, column_name: str):
        """
        Perform an aggregation operation on the data.
        """
        query = f"""
        SELECT {aggregation_function}({column_name})
        FROM {table_name}
        """
        result: taos.TaosResult = self.conn.query(query)

    @staticmethod
    def write_to_table(self, conn, table, tags, lines_set):
        cursor = conn.cursor()
        for line in list(lines_set):
            processed_line = f"INSERT INTO {table} USING {self.stable_name} TAGS({tags}) VALUES {line};"
            cursor.execute(processed_line)
        cursor.close()
        conn.close()
    
    def write_balanced_sets_to_table_parallel(self, table: str, number: int, path: str, host: str = "localhost", port: int = 6030, user: str = "root", password: str = "taosdata"):
        """
        Writes balanced sets of lines from a file to a table using multiple connections in parallel.

        Parameters:
        - table (str): The name of the table to write to.
        - number (int): The number of sets (and connections) to use.
        - path (str): The path to the file.
        - host, port, user, password: Connection parameters for the database.
        """
        # Get the balanced sets of lines
        sets_list = get_balanced_sets(number, path)
        
        # Use ThreadPoolExecutor to parallelize the writing process
        with concurrent.futures.ThreadPoolExecutor(max_workers=number) as executor:
            futures = []
            for lines_set in sets_list:
                conn = taos.connect(host=host, port=port, user=user, password=password)
                futures.append(executor.submit(self.write_to_table, self, conn, table, tags, lines_set))
            
            # Wait for all threads to complete
            for future in concurrent.futures.as_completed(futures):
                future.result()


def setup():
    my_object = TDengine(database_name, retention_time,stable_name, schema, tags, tables)
    my_object.create_stable()
    my_object.create_tables()
    return my_object

def get_file_paths(base_dir, dir_name):
    dir_path = os.path.join(base_dir, dir_name)
    file_names = os.listdir(dir_path)
    file_paths = [os.path.join(dir_path, file_name) for file_name in file_names]
    return sorted(file_paths)


if __name__ == "__main__":
   
    my_object = setup()
    # Batch Tests
    copy_files_lambda2 = lambda: my_object.copy_files(ordered_tags_list, get_file_paths(BASE_DIR, '1klines'))
    print("---COPY time: %s seconds ---" % timeit.timeit(copy_files_lambda2, number=1))

    # Write Parallel Lines 
    # my_object.write_balanced_sets_to_table_parallel()

    # Queries Tests
    # The 1k lines files starts in '2012-01-03 01:00:00.000'
    # and end in '2012-01-03 01:00:33.300'
    # For all the test the interval must be here (?)
    interval_query_lambda = lambda: my_object.interval_query(tables[0]['name'], '2012-01-03 01:00:26.200', '2012-01-03 01:00:29.700')
    print("---Interval query time: %s seconds ---" % timeit.timeit(interval_query_lambda, number=1))

    exact_query_lambda = lambda: my_object.exact_query(tables[0]['name'], '2012-01-03 01:00:27.233')
    print("---Exact query time: %s seconds ---" % timeit.timeit(exact_query_lambda, number=1))

    agg_query_lambda = lambda: my_object.aggregation_query(tables[0]['name'], 'AVG', 'frequency')
    print("---Interval query time: %s seconds ---" % timeit.timeit(agg_query_lambda, number=1))

    '''
    • Qual é o valor da magnitude no instante '2012/01/03 01:02:24.833'?
    • Qual é o valor médio do ângulo ao longo de todo o conjunto de dados?
    • Qual é o valor médio do ângulo no intervalo de tempo entre '2012/01/03 01:00:24.000'
    e '2012/01/03 01:02:24.833'?
    • Quais são os registros existentes no período entre '2012/01/03 01:00:24.000' e
    '2012/01/03 01:02:24.833'
    '''
    query = '''
    SELECT *
    FROM city01
    LIMIT 10
    '''
    my_object.query(query)

    # Tear Down
    my_object.drop_database()
    my_object.close_conn()
