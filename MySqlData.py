import pymysql
from datetime import datetime
from decimal import Decimal
import time
import yaml


class MySQL:
    conn = None
    host = ""
    user = ""
    password= ""
    database= ""
    def __init__(self) -> None:
        with open("etc/dbconfig.yml") as stream:
            try:
                config = yaml.safe_load(stream)
                self.host = config['mysql']['host']
                self.user = config['mysql']['user']
                self.password = config['mysql']['password']
                self.database = config['mysql']['db']
            except Exception  as ex:
                print(ex)
        pass
    
    def __get_connection(self):
        for i in range(0,5):
            try:
                connection = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    connect_timeout=60
                )
            except:
                print('mysql connect fail, wait 120 sec and try %s times.' %i)
                if i == 4:
                    raise
                time.sleep(120)
        return connection
    
    def get_all_station(self, table_name):
        '''
        取出所有測站代碼
        '''
        conn = self.__get_connection()
        cur = conn.cursor()#pymysql.cursors.DictCursor)
        query = "select distinct(Stno) from {}".format(table_name)
        cur.execute(query)
        stno = cur.fetchall()
        conn.close()
        return stno

    def get_all_obstime(self, table_name):
        '''
        取出資料表中所有觀測時間
        '''
        conn = self.__get_connection()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        query = "select distinct(obstime) obstime from {}".format(table_name)
        cur.execute(query)
        obstimes = cur.fetchall()
        conn.close()
        
        return obstimes
        
    def get_data_by_obstime(self, table_name, obstime, columns):
        '''
        依據觀測時間抓取資料表中的所有資料
        '''
        if self.conn == None:
            conn = self.__get_connection()
        elif self.conn.open == False:
            conn = self.__get_connection()
        else:
            conn = self.conn
            
        cur = conn.cursor() #pymysql.cursors.DictCursor
        query = ("select %s from %s where obstime = '%s'") % (columns, table_name, obstime)
        #query = ("select stno, obstime, stnpres, tx, wd, ws, precp, sunshine, rh, recupdtime from %s where obstime = %s") % (table_name, obstime)
        cur.execute(query)
        datas = cur.fetchall()
        
        return datas
    
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
    