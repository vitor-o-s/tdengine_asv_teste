import database_manager as db_manager

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



class TDengine():
    
    def __init__(self,
                database: str,
                retention_time: str,
                stable_name: str, 
                schema: str, 
                tags: str,
                tables: list, 
                **kwargs) -> None:
        self.conn = db_manager.open_connection()
        self.database = database
        self.retention_time = retention_time
        self.stable_name = stable_name
        self.schema = schema
        self.tags = tags
        self.tables = tables
        db_manager.create_database(self.conn, self.database, self.retention_time)
        db_manager.use_database(self.conn, self.database)
    
    def create_stable(self):
        return db_manager.create_stable(self.conn,
                                        self.stable_name,
                                        self.schema,
                                        self.tags)
    
    def create_tables(self):
        for item in self.tables:
            db_manager.create_table(self.conn,
                                    item['name'],
                                    self.stable_name,
                                    item['tag'])
    
    def copy_data(self, table, path):
        db_manager.insert_file(self.conn, table, path)
    
    def copy_files(self, tables, paths):
        db_manager.insert_multiple_files(self.conn, tables, paths)

    def close_conn(self):
        return db_manager.close_connection(self.conn)
    
    def drop_database(self):
        return db_manager.drop_database(self.conn, self.database)


    def query(self, query: str):
        return db_manager.query(self.conn, query)

def setup():
    my_object = TDengine(database_name, retention_time,stable_name, schema, tags, tables)
    my_object.create_stable()
    my_object.create_tables()
    return my_object



if __name__ == "__main__":
   
    my_object = setup()
    # Test
    my_object.copy_data(tables[0]['name'], BASE_DIR + '1klines/first_loc.csv')
    my_object.copy_files(
        ["city01", "city02", "city03", "city04", "city05", "city06"],
        [BASE_DIR + '1klines/first_loc.csv',
         BASE_DIR + '1klines/second_loc.csv',
         BASE_DIR + '1klines/third_loc.csv',
         BASE_DIR + '1klines/fourth_loc.csv',
         BASE_DIR + '1klines/fifth_loc.csv',
         BASE_DIR + '1klines/sixth_loc.csv']
        )

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