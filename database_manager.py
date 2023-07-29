import taos

def open_connection(host: str = "localhost",
                    port: int = 6030,
                    user: str = "root",
                    password: str = "taosdata"):
    conn = taos.connect(host=host,
                        port=port,
                        user=user,
                        password=password)
    
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    return conn

def close_connection(conn: taos.TaosConnection):
    print('Close connection!')
    return conn.close()

def create_database(conn: taos.TaosConnection, 
                    database_name: str,
                    retention_time: str):
    conn.execute("CREATE DATABASE IF NOT EXISTS "+ database_name+
                 " KEEP "+ retention_time)

def use_database(conn: taos.TaosConnection, 
                    database_name: str):
    conn.select_db(database_name) # execute("USE "+ database_name)

def drop_database(conn: taos.TaosConnection, 
                    database_name: str):
    conn.execute("DROP DATABASE IF EXISTS "+ database_name)

def create_stable(conn: taos.TaosConnection,
                  stable_name: str,
                  schema: str,
                  tags: str):
    conn.execute("CREATE STABLE IF NOT EXISTS "+ stable_name +" "+ schema +" "
                 "TAGS ("+ tags+ ");")

def create_table(conn: taos.TaosConnection,
                 table_name: str,
                 stable_name: str,
                 tags: str):
    conn.execute("CREATE TABLE IF NOT EXISTS "+ table_name + " USING "
                 + stable_name + " TAGS ("+ str(tags) +");")
    
def insert_file(conn: taos.TaosConnection,
                table_name: str,
                path: str):
    conn.execute("INSERT INTO "+ table_name +" FILE '"+ path +"';")

def insert_multiple_files(
        conn: taos.TaosConnection,
        tables_name: list[str],
        paths: list[str]
        ):
    query = "INSERT INTO "
    for table, path in zip(tables_name, paths):
        query += table +" FILE '"+ path +"' "
    query += ";"
    # print(query)
    conn.execute(query)

def insert_row(conn: taos.TaosConnection,
                table_name: str,
                values: str):
    conn.execute("INSERT INTO "+ table_name +" VALUES ()"+ values +");")

def query (conn: taos.TaosConnection,
           query: str):
    result: taos.TaosResult = conn.query(query)
    print('FIELD COUNT:', result.field_count)
    print('ROW_COUNT:', result.row_count)
    rows = result.fetch_all_into_dict()
    print('ROW_COUNT:', result.row_count)
    for row in result:
        print('ROW:',row)