import time
import os
import pymongo
from lib.mongo import Mongo
import pymysql
import random
import math
from datetime import datetime, timedelta

class MySQL:
    def __init__(self) -> None:
        pass
    
    def get_connection(self):
        return self.__get_connection()
    
    def __get_connection(self):
        
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="Ie@123456",
            database="ari"
        )
        return connection
    
    def get_station_id(self, station_type):
        
        if station_type == "cwa":
            station_type = 1
        elif station_type == "auto":
            station_type = 3
        elif station_type == "agr":
            station_type = "4, 6, 7"
        else:
            return
        query = "SELECT id FROM Station where station_type in (%s)"% station_type
        
        conn  = self.__get_connection()
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        results  = [row[0] for row in results] 
        print('random 出的前10筆測站id')
        for i in range(10):
            print(results[i])
        return results
        
    
class RandGen:
    def __init__(self) -> None:
        pass    
    
    def random_station(self,station_list, rand_count):
        s_count = len(station_list)
        
        result = []
        for i in range(0, rand_count):
            n = random.randint(0, s_count - 1)
            s = station_list[n]
            result.append(s)        
        return result
    
    def random_obstime(self, rand_count):
        start_date = datetime(2022, 1, 1, 0, 0)
        end_date = datetime(2023, 10, 31, 23, 0)

        # 生成 n 个随机时间点
        random_time_points = [start_date + timedelta(hours=random.randint(0, int((end_date - start_date).total_seconds() / 3600))) for _ in range(rand_count)]

        return random_time_points
    
def test1_get_obstime_and_stationid():
    ranGen = RandGen()
    mySQL = MySQL()
    mongo = Mongo()
    mongo.mongo_conn("localhost:27017,localhost:37017")
    table_name_list = ['cwbhour'] #, 'autoprechour', 'agrhour']
    station_type_list = ['cwa'] #, 'auto', 'agr']
    random_count = 50000
    
    for s in zip(station_type_list, table_name_list):
        station_type = s[0]
        table_name = s[1]
        
        station_list = mySQL.get_station_id(station_type=station_type)
        rand_station = ranGen.random_station(station_list,random_count)
        rand_obstime = ranGen.random_obstime(random_count)
        
        fail_list = []
        succ_data = []
        start = time.time()
        for rand in zip(rand_station, rand_obstime):    
            r = mongo.mongo_find("db1", table=table_name, where={"Stno": rand[0], "ObsTime": rand[1]})
            if len(r) == 0:
                fail_list.append(rand)
            else:
                succ_data.append(r)
        
        end = time.time()
        print("MongoDB: %s" %table_name)
        print('  get data rows: %s,  fail: %s' % (len(succ_data), len(fail_list)))
        print("  cost: %s sec (%s min)" % ((end - start), (end - start)/60))
        
        
        fail_list = []
        succ_data = []            
        start = time.time()
        conn = mySQL.get_connection()
        for rand in zip(rand_station, rand_obstime):
            query = ("SELECT * FROM %s where stno = '%s' and obstime = '%s'" % (table_name, rand[0], rand[1]))                
            
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            if len(results) == 0:
                fail_list.append(rand)
            else:
                succ_data.append(results)
        end = time.time()
        
        print("MySQL: %s" %table_name)
        print("  get data rows: %s, fail: %s" %(len(succ_data), len(fail_list)))
        print("  cost: %s sec (%s min)" %((end - start), (end-start) / 60))
                
        

        
        
    
if __name__ == '__main__':  
    test1_get_obstime_and_stationid()
    
    # mySQL = MySQL()
    # mongo = Mongo()
    # #mongodb = MongoDB()
    # #mongodb.select1('cwbhour')
    # #mongodb.select('cwbhour', '466880', '2022-01-01T01:00:00')
    # table_name_list = ['cwbhour', 'autoprechour', 'agrhour']
    # station_type_list = ['cwa', 'auto', 'agr']
    
    # # for i in zip(table_name_list, station_type):
    # #     print(i)
    # #     print(i[0])
    
    # station_type = 'cwa'
    # table_name = 'cwbhour'
    # random_count = 5000
    # station_list = mySQL.get_station_id(station_type)    
    # rand_station = mySQL.random_station(station_list,random_count)
    # rand_obstime = mySQL.random_obstime(random_count)
    # fail_list = []
    # succ_data = []
    # for i in range(0, 10):
        
    #     start = time.time()
    #     for rand in zip(rand_station, rand_obstime):    
    #         r = mongo.mongo_find("db1", table=table_name, where={"Stno": rand[0], "ObsTime": rand[1]})
            
    #         #print(r)
    #         #os.system('pause')
    #         #r = mongodb.select(table_name, rand[0], rand[1])
    #         if len(r) == 0:
    #             fail_list.append(rand)
    #         else:
    #             succ_data.append(r)
    #     end = time.time()
            
    #     print('get data rows: %s,  fail: %s' % (len(succ_data), len(fail_list)))
    #     print("cost: %s sec (%s min)" % ((end - start), (end - start)/60))
        