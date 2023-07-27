import taos
# from logging import Logger

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

def insert_row(conn: taos.TaosConnection,
                table_name: str,
                values: str):
    conn.execute("INSERT INTO "+ table_name +" VALUES ()"+ values +");")

'''def time_retention(conn: taos.TaosConnection,
                    database_name: str,
                    values: int = 36500):
    conn.execute("ALTER DATABASE "+ database_name +" KEEP "+ str(values)+";")'''

def query (conn: taos.TaosConnection,
           query: str):
    print('QUERY RESULT:',conn.query(query))