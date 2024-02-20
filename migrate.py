import os
import time
from datetime import datetime
from decimal import Decimal
import pymysql
#import pymongo
from lib.mongo import Mongo, MongoSchema

class MySQLConnect:
    
    def __init__(self) -> None:
        pass
    
    def __get_connection(self):
        
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="Ie@123456",
            database="ari"
        )
        return connection
    
    def get_mysql_total_rows(self, table_name):
        conn = self.__get_connection()
        cur = conn.cursor()
        count_query = "select count(*) from {}".format(table_name)
        cur.execute(count_query)
        total_rows = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return total_rows[0]
        
    def get_mysql_data(self, table_name):
        connection = self.__get_connection()
        cur = connection.cursor()
        
        
        col_names = self.get_table_columns(cur)
        
        total_rows = self.get_mysql_total_rows(table_name)
        
        # count_query = "select count(*) from {}".format(table_name)
        # cur.execute(count_query)
        # total_rows = cur.fetchone()
        
        limit = 10000
        for i in range(0, total_rows, limit):
            offset = i
            query = "select * from {} limit {}, {}".format(table_name, offset, limit)
            cur.execute(query)
            results = cur.fetchall()
            jsons = self.convert_to_json(results, col_names)
            yield jsons
            # os.system('pause')        
        
                
        cur.close()
        connection.close()        
        
    def get_table_columns(self, cur):
        col_query = "SELECT * FROM {} limit 1".format(table_name)
        cur.execute(col_query)
        # results = cur.fetchall()
        columns = [col[0] for col in cur.description]
        return columns
        
    def column_escape(self, row, col_names):
        
        row_dict = dict(zip(col_names, row))
        
        for key, value in row_dict.items():
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[key] = float(value)
            
        return row_dict
        
    def convert_to_json(self, datas, col_names):
        res_json = []
        for row in datas:
            res_json.append(self.column_escape(row, col_names))
            
        return res_json
    
    
if __name__ == '__main__':
    mysql = MySQLConnect()
    mongodb = Mongo()
    #mongodb.mongo_conn(host="localhost:27017,localhost:37017,localhost:47017,localhost:17017")
    mongodb.mongo_conn(host="localhost:27017,localhost:37017")
    mSchema = MongoSchema()
    table_name_list = ['cwbhour'] #, 'agrhour', 'autoprechour'
    db = 'db1'
    for table_name in table_name_list:
        mSchema.create_index(db, table_name, [("Stno", 1),("ObsTime", 1)], unique=True)
        
        mongo_cost = 0
        mongo_cost_list = []
        start = time.time()
        for data in mysql.get_mysql_data(table_name):
            m_start = time.time()
            mongodb.mongo_insert(db, table_name, data) 
            m_end = time.time()
            cost = m_end - m_start
            mongo_cost_list.append(cost* 1000)
            mongo_cost = mongo_cost + cost 
            
        end = time.time()

        total_rows = mysql.get_mysql_total_rows(table_name)
        print("migrate %s:\n  total rows: %s \n  total cost: %s sec (%s min)\n  mongo cost: %s sec (%s min)" %(table_name, total_rows, (end - start), (end - start) / 60, mongo_cost, mongo_cost / 60 ))
        print("each insert cost is (ms)")
        log = ""
        count =""
        count2 = ""
        
        for i in range(1, len(mongo_cost_list)):
            count2 += "%s," %i

        for c in mongo_cost_list:
            log += ("%s, " %c )

        for i in range(0, len(mongo_cost_list)):
            count += "1,"
            
        print(count2)
        print(log)
        print(count)
        print(' ----------------------------------------- ----------------------------------------- ')
    
    
    
    
        