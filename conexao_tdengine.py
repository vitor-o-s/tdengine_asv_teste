import taos
import os
# import timeit 

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
        # print("Running query:\n",create_stable_query)
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
        for row in result:
            print('ROW:',row)

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
    # Test
    my_object.copy_files([tables[0]['name']], [BASE_DIR + '1klines/1_loc.csv'])
    my_object.copy_files(ordered_tags_list, get_file_paths(BASE_DIR, '1klines'))
    # copy_files_lambda2 = lambda: my_object.copy_files(ordered_tags_list, get_file_paths(BASE_DIR, '1klines'))
    # print("--- %s seconds ---" % timeit.timeit(copy_files_lambda2, number=1))

    query = '''
    SELECT *
    FROM city01
    '''
    my_object.query(query)

    # Tear Down
    my_object.drop_database()
    my_object.close_conn()

'''
Timestamp, Austin_V1LPM_Magnitude,Austin_V1LPM_Angle,
HARRIS_V1LPM_Magnitude,HARRIS_V1LPM_Angle,
McDonald 1P_V1LPM_Magnitude,McDonald 1P_V1LPM_Angle,
UT 3 phase_VALPM_Magnitude,UT 3 phase_VALPM_Angle,
UT Pan Am_V1LPM_Magnitude,UT Pan Am_V1LPM_Angle,
WACO_V1YPM_Magnitude,WACO_V1YPM_Angle,
Z_UT_3378_AO[079]_Value,
Z_UT_3378_AO[085]_Value,
Z_UT_3378_AO[087]_Value,
Z_UT_3378_AO[090]_Value,
Z_UT_3378_AO[091]_Value,
Z_UT_3378_AO[092]_Value


import taos

lines = ["d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,'California.SanFrancisco',2",
         "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,'California.LosAngeles',3",
         "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,'California.LosAngeles',2",
         "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,'California.LosAngeles',3",
         "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,'California.SanFrancisco',3",
         "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,'California.SanFrancisco',2",
         "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,'California.SanFrancisco',2",
         "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,'California.LosAngeles',2"]

# The generated SQL is:
# INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
#             d1002 USING meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
#             d1003 USING meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
#             d1004 USING meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)

def get_sql():
    global lines
    lines = map(lambda line: line.split(','), lines)  # [['d1001', ...]...]
    lines = sorted(lines, key=lambda ls: ls[0])  # sort by table name
    sql = "INSERT INTO "
    tb_name = None
    for ps in lines:
        tmp_tb_name = ps[0]
        if tb_name != tmp_tb_name:
            tb_name = tmp_tb_name
            sql += f"{tb_name} USING meters TAGS({ps[5]}, {ps[6]}) VALUES "
        sql += f"('{ps[1]}', {ps[2]}, {ps[3]}, {ps[4]}) "
    return sql


def insert_data(conn: taos.TaosConnection):
    sql = get_sql()
    affected_rows = conn.execute(sql)
    print("affected_rows", affected_rows)  # 8


if __name__ == '__main__':
    connection = get_connection()
    try:
        create_stable(connection)
        insert_data(connection)
    finally:
        connection.close()
'''