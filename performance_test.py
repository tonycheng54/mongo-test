import os
import yaml
from lib.mongo import Mongo, MongoSchema, Opretor
from dateutil.relativedelta import relativedelta
from MySqlData import MySQL
from lib.data_arrange import MongoDataArrange
from lib.mongoschema import Schema
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

runtype = 'TEST' #'TEST' 'DEV'

def show(filename, string):
    print(string)
    f = open('log/%s'%filename, 'a')
    f.write(string + '\n')
    f.close()

def print_cost_time(filename, total_sec, round_cost:list, action_rows: list, informations: list):
    show(filename, 'total cost: %s' %(total_sec))
    
    if len(informations) == 0:
        informations = [''] * len(round_cost) 
    unit_list = ['1'] * len(round_cost)
    show(filename,'round, information, cost, effect_rows, weight')
    count = 0
    for c in zip(informations, round_cost, action_rows, unit_list):
        show(filename, '%s, %s, %s, %s, %s' %(count, c[0], c[1], round(c[2],2), c[3]))
        count += 1

def column_escape(row, col_names):
    row_dict = dict(zip(col_names, row))
    
    for key, value in row_dict.items():
        if isinstance(value, Decimal):
            row_dict[key] = float(value)
        
    return row_dict   


class MongoInsertTest:
    db = ""
    collection = ''
    conn_str = ''
    def __init__(self, conn_str, db, collection) -> None:
        self.conn_str = conn_str
        self.db = db
        self.collection = collection
        pass

    def insert_start(self):
        mysql = MySQL()
        mysql_test_table = 'autoprechour'

        mongodb = Mongo()
        client = mongodb.mongo_conn(host=self.conn_str)
        db = self.db
        mongo_table = self.collection

        mSchema = MongoSchema(client)
        mSchema.create_index(self.db, self.collection, [(Schema.obstime, 1)])
        mSchema.create_index(db, mongo_table, [(Schema.stationid, 1)])


        count = 0
        obstimes = mysql.get_all_obstime(mysql_test_table)
        round_history = []
        informations = []
        action_rows=[]
        total_start = time.time()
        try:
            for obstime in obstimes:
            # 回圈逐次取出該觀測資時間點的所有資料
            
                datas = mysql.get_data_by_obstime(mysql_test_table, obstime['obstime'].isoformat(), 'Stno, ObsTime, StnPres, Tx, WD, WS, Precp, sunshine, RH, RecUpdTime')
                #query = ("select stno, obstime, stnpres, tx, wd, ws, precp, sunshine, rh, recupdtime from %s where obstime = %s") % (table_name, obstime)
                obsdatas = []
                ids = []
                for data in datas:
                    # 各測站資料取出並整理成 mongo shcema
                    arrange = MongoDataArrange()
                    dic = dict()
                    data = column_escape(data, ['Stno', 'ObsTime', 'StnPres', 'Tx', 'WD', 'WS', 'Precp', 'sunshine', 'RH', 'RecUpdTime'])
                    dic = arrange.base_data(dic, data['Stno'], data['ObsTime'])
                    dic = arrange.pressure_arrange(dic, data['StnPres'], data['RecUpdTime'])
                    dic = arrange.tx_arrange(dic, data['Tx'], data['RecUpdTime'])
                    dic = arrange.wind_arragen(dic, data['WD'], data['WS'], wg= None, updatetime= data['RecUpdTime'])
                    dic = arrange.rain_arrange(dic, data['Precp'], data['RH'], updatetime= data['RecUpdTime'])
                    ids.append({Schema.id: dic[Schema.id]})
                    obsdatas.append(dic)
                round_start = time.time()
                mongodb.mongo_insert(db, mongo_table, obsdatas)
                mongodb.mongo_update_multiple(db, mongo_table, ids, obsdatas)

                round_end = time.time()
                info = "%s" % (data['ObsTime'])
                informations.append(info)
                action_rows.append(len(datas))

                round_history.append((round_end - round_start) * 1000)
                show('insert_process.csv', "cost: %s,    insertrows: %s,    obstime: %s" %((round_end - round_start) * 1000, len(datas), data['ObsTime']))
                #print(mongodb.mongo_conn_state())
                if runtype == 'DEV':
                    count+=1
                    #print('  data len: %s' %count)
                    if count >= 10:
                        break
            total_end = time.time()

        except Exception as ex:
            print(ex)
            mongodb.mongo_insert('log', 'errmsg', [{"time":datetime.now(), "type": "MongoInsertTest", "msg": ex}])
            total_end = time.time()


        return (total_end - total_start) * 1000, round_history, action_rows, informations

class MongoInsertHistory:
    db = ""
    collection = ""
    conn_str = ''
    def __init__(self, conn_str, db, collection) -> None:
        self.conn_str = conn_str
        self.db = db
        self.collection = collection
        pass
    
    def start_test(self):
        mysql = MySQL()
        mysql_test_table = 'autoprechour'

        mongodb = Mongo()
        mongodb.mongo_conn(host=self.conn_str)
        db = self.db
        mongo_table = self.collection

        count = 0
        obstimes = mysql.get_all_obstime(mysql_test_table)
        round_history = []
        informations = []
        action_rows=[]
        total_start = time.time()
        try:
            for obstime in obstimes:
            # 回圈逐次取出該觀測資時間點的所有資料
                datas = mysql.get_data_by_obstime(mysql_test_table, obstime['obstime'].isoformat(), 'Stno, ObsTime, StnPres, Tx, WD, WS, Precp, sunshine, RH, RecUpdTime')
                #query = ("select stno, obstime, stnpres, tx, wd, ws, precp, sunshine, rh, recupdtime from %s where obstime = %s") % (table_name, obstime)
                obsdatas = []
                ids = []
                for data in datas:
                    # 各測站資料取出並整理成 mongo shcema
                    updatetime = datetime.now()
                    arrange = MongoDataArrange()
                    dic = dict()
                    data = column_escape(data, ['Stno', 'ObsTime', 'StnPres', 'Tx', 'WD', 'WS', 'Precp', 'sunshine', 'RH', 'RecUpdTime'])
                    #dic = arrange.base_data(dic, data['Stno'], data['ObsTime'])
                    id = arrange.get_id(data['Stno'], data['ObsTime'])
                    dic = arrange.pressure_arrange(dic, data['StnPres'], updatetime, isappend=True)
                    dic = arrange.tx_arrange(dic, data['Tx'], updatetime, isappend=True)
                    dic = arrange.wind_arragen(dic, data['WD'], data['WS'], wg= None, updatetime= updatetime, isappend=True)
                    dic = arrange.rain_arrange(dic, data['Precp'], data['RH'], updatetime= updatetime, isappend=True)
                    
                    obsdatas.append(dic)
                    ids.append({Schema.id: id})
                round_start = time.time()
                mongodb.mongo_update_append(db, mongo_table, ids, obsdatas)
                #mongodb.mongo_insert(db, mongo_table, obsdatas)
                round_end = time.time()
                info = "%s" % (data['ObsTime'])
                informations.append(info)
                action_rows.append(len(datas))

                round_history.append((round_end - round_start) * 1000)
                show('append_history_process.csv', "cost: %s,    insertrows: %s,    obstime: %s" %((round_end - round_start) * 1000, len(datas), data['ObsTime']))
                if runtype == 'DEV':
                    count+=1
                    #print('  data len: %s' %count)
                    if count >= 10:
                        break
            total_end = time.time()
        except Exception as ex:
            print(ex)
            mongodb.mongo_insert('log', 'errmsg', [{"time":datetime.now(), "type": "MongoInsertHistory", "msg": ex}])
            total_end = time.time()

        return (total_end - total_start) * 1000, round_history, action_rows, informations

class MongoSelectEachMonth:
    db = ''
    collection = ''
    conn_str = ''
    def __init__(self, conn_str,db, collection) -> None:
        self.conn_str = conn_str
        self.db = db 
        self.collection = collection
        pass
    def start_test(self):
        tz = timezone(timedelta(hours=8))
        mongodb = Mongo()
        mongodb.mongo_conn(host=self.conn_str)
        db = self.db
        table = self.collection
        round_list = []
        action_rows = []
        
        start = time.time()
        for year in range(2022,2024):
            for month in range(1, 13):
                start_dt = datetime(year, month, 1,0,0,0,tzinfo=tz)
                #end_dt = start_dt + relativedelta(months=1)
                end_dt = start_dt + relativedelta(days=1)
                try:
                    round_start = time.time()
                    datas = mongodb.mongo_find(db, table, where={Schema.obstime:{Opretor.gte:start_dt, Opretor.lt:end_dt}},
                                               column={Schema.id:1, 
                                                       Schema.obstime:1,
                                                       Schema.stationid:1,}, print_flag=True)
                    round_end = time.time()
                    print(len(datas))
                except Exception as ex:
                    print(start_dt)
                    print(end_dt)
                    raise ex
                round_list.append((round_end - round_start) * 1000)
                action_rows.append(len(datas))
        end = time.time()
        return (end-start)*1000, round_list, action_rows

class MongoSelectEachStation:
    db = ''
    collection = ''
    conn_str = ''
    def __init__(self, conn_str, db, collection) -> None:
        self.conn_str = conn_str
        self.db = db
        self.collection = collection
        pass
    def start_test(self):
        mysql = MySQL()
        mysql_test_table = 'autoprechour'
        stnos = mysql.get_all_station(mysql_test_table)

        mongodb = Mongo()
        mongodb.mongo_conn(host=self.conn_str)
        db = self.db
        mongo_table = self.collection

        round_list = []
        action_rows = []
        informations = []
        start = time.time()
        for stno in stnos:
            #print(stno[0])
            round_start = time.time()
            datas = mongodb.mongo_find(db, mongo_table, where={Schema.stationid:stno[0]})
            round_end = time.time()

            round_list.append((round_end - round_start) * 1000)
            action_rows.append(len(datas))
            informations.append(stno[0])
        end = time.time()
        return (end - start) * 1000, round_list, action_rows, informations


class TestQCUpdate:
    db = ""
    collection = ''
    conn_str = ''
    def __init__(self, conn_str, db, collection) -> None:
        self.conn_str = conn_str
        self.db = db
        self.collection = collection

    def test(self):
        mysql = MySQL()
        mysql_test_table = 'autoprechour'
        #stnos = mysql.get_all_station(mysql_test_table)
        stnos = ['C0A520', 'C0A530']

        mongodb = Mongo()
        mongodb.mongo_conn(host=self.conn_str)
        db = self.db
        mongo_table = self.collection
        arrange = MongoDataArrange()
        for day in range(1, 10):
#       for day in range(1, 32):
            for hour in range(0, 24):
                obstime = datetime(2022, 1, day, hour, 0, 0, tzinfo=timezone(timedelta(hours=8)))
                datas = mongodb.mongo_find(db, mongo_table, where={Schema.stationid:{Opretor.In:stnos}, Schema.obstime:obstime})
                if len(datas) == 0:
                    print("no datas.   obstime: %s" %( obstime))
                    continue
                else:
                    print(" obstime: %s"%(obstime))
                update_datas = []
                where_datas = []
                for data in datas:
                    #print(data)
                    #data[Schema.Rain.qc_key] = arrange.qc_arrange(['cond', 'qc_method1'], ['success', 'fail'])
                    qc = arrange.qc_arrange(['多彩品管1', '多彩品管2'], ['Q', 'A'])  
                    # print("qcdata:")                      
                    # print(qc)
                    update = {Schema.id:data[Schema.id], Schema.Rain.qc_key:qc}
                    update = {"%s.0.%s"%(Schema.Rain.key,Schema.Rain.qc_key):{
                                    "$each":qc
                                    }
                                }
                    #print(update)
                    update_datas.append(update)
                    where = {Schema.id:data[Schema.id]}
                    where_datas.append(where)
                    
                    
                os.system("pause")
                mongodb.mongo_update_append(db, mongo_table, where=where_datas, update=update_datas)

class TestAppendObsDataHistory:
    db = ""
    collection = ''
    conn_str = ''
    def __init__(self, conn_str, db, collection) -> None:
        self.conn_str = conn_str
        self.db = db
        self.collection = collection
    
    def test(self):
        mysql = MySQL()
        mysql_test_table = 'autoprechour'
        #stnos = mysql.get_all_station(mysql_test_table)
        stnos = ['C0A520', 'C0A530']
        obstimes = mysql.get_all_obstime(mysql_test_table)

        mongodb = Mongo()
        mongodb.mongo_conn(host=self.conn_str)
        db = self.db
        mongo_table = self.collection
        arrange = MongoDataArrange()

        for obstime in obstimes:
            datas = mysql.get_data_by_obstime(mysql_test_table, obstime['obstime'].isoformat(), 'Stno, ObsTime, StnPres, Tx, WD, WS, Precp, sunshine, RH, RecUpdTime')
            obsdatas = []
            ids = []
            for data in datas:
                updatetime = datetime.now
                dic = dict()
                id = arrange.get_id(data['Stno'], data['ObsTime'])
                dic = arrange.rain_arrange(dic, data['Precp'], data['RH'], updatetime= updatetime, isappend=False)
                obsdatas.append(dic)
                ids.append({Schema.id: id})
                mongodb.mongo_update_append(db,)


if __name__ == '__main__':

    with open("etc/dbconfig.yml") as stream:
        try:
            config = yaml.safe_load(stream)
        
            print(config['mongo']['host'])
            print(config['mongo'])
            print(config['mysql'])
        except Exception as ex:
            print(ex)

    mongo_table = config['mongo']['collection']
    db = config['mongo']['db']
    conn_str = config['mongo']['host']
    test_item = config['test']
 
    #conn_str = '192.168.60.21:27017,192.168.60.22:27017'
    if test_item['insert'] == True:
        insert_test = MongoInsertTest(conn_str, db, mongo_table)
        totalcost, round_history, action_rows, informations  = insert_test.insert_start()
        print_cost_time('insert.csv', total_sec=totalcost, round_cost=round_history, action_rows=action_rows, informations=informations)
    
    if test_item['month'] == True:
        test = MongoSelectEachMonth(conn_str, db, mongo_table)
        totalcost, round_list, action_rows = test.start_test()
        print_cost_time('select_month.csv', total_sec=totalcost, round_cost=round_list, action_rows=action_rows, informations= [])

    if test_item['station'] == True:
        test = MongoSelectEachStation(conn_str, db, mongo_table)
        totalcost, round_history, action_rows, informations = test.start_test()
        print_cost_time('select_station.csv',total_sec=totalcost, round_cost=round_history, action_rows=action_rows, informations=informations)

    if test_item['update'] == True:
        test = MongoInsertHistory(conn_str, db, mongo_table)
        totalcost, round_history, action_rows, informations = test.start_test()
        print_cost_time('append_history.csv',total_sec=totalcost, round_cost=round_history, action_rows=action_rows, informations=informations)

    test = TestQCUpdate(conn_str, db, mongo_table)
    test.test()
