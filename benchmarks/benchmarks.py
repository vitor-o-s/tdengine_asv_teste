# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
import sys
# print(sys.path)
sys.path.append("..")
# sys.path.insert(0, '/home/dell/tcc_package/tdengine_asv_teste/conexoes/')
# print(sys.path)
# from conexoes.conexao_tdengine import TDengine, tables, BASE_DIR, ordered_tags_list, get_file_paths, database_name, retention_time, stable_name, schema, tags
from conexoes.conexao_tdengine import TDengine
# from conexao_tdengine import TDengine# conexao_tdengine

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

class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    """def setup(self):
        self.d = {}
        for x in range(500):
            self.d[x] = None
    """
    def setup(self):
        self.my_object = TDengine(database_name, retention_time,stable_name, schema, tags, tables)
        self.my_object.create_stable()
        self.my_object.create_tables()
    
    def time_copy_files(self):
        self.my_object.copy_files(ordered_tags_list, get_file_paths(BASE_DIR, '1klines'))

"""    def time_keys(self):
        for key in self.d.keys():
            pass

    def time_values(self):
        for value in self.d.values():
            pass

    def time_range(self):
        d = self.d
        for key in range(500):
            x = d[key]
"""

'''
class MemSuite:
    def mem_list(self):
        return [0] * 256
'''