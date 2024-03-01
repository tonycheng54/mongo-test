import os
import yaml
import csv
from lib.mongo import Mongo, MongoSchema, Opretor
from dateutil.relativedelta import relativedelta
from MySqlData import MySQL
from lib.data_arrange import MongoDataArrange
from lib.mongoschema import Schema
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

class ObsProcessSample:
    db = ""
    collection = ''
    conn_str = ''
    def __init__(self, conn_str, db, collection) -> None:
        self.conn_str = conn_str
        self.db = db
        self.collection = collection
        pass

    def insert_obsdata_from_pri(self, parsed_data, round=1):
        '''
        收到pri檔後，經parsing處理重複資料後進行插入。
        '''
        mongodb = Mongo()
        conn = mongodb.mongo_conn(self.conn_str)
        arrange = MongoDataArrange()
        insert_data = []
        where_data = []
        for data in parsed_data:
            if round == 1:
                id = arrange.get_id(data['stno'], data['obstime'], timezone='UTC')
                dic = arrange.base_data(dict(), data['stno'], data['obstime'], timezone='UTC')
                # 同時插入
                dic = arrange.pressure_arrange(dic, datetime.now(), data['pressure'], ['即時品管-氣壓'], ['pass'])
                #dic = arrange.qc_arrange1(dic, Schema.Pressure.key, ['即時品管-氣壓'], ['pass'])
                dic = arrange.rain_arrange(dic, datetime.now(), data['rain'], data['rh'])
                # 不同時間插入自行處理
                dic[Schema.Rain.key][0][Schema.Rain.QC.key] = arrange.qc_arrange(['即時品管-雨量'], ['pass'])
            else:
                id = arrange.get_id(data['stno'], data['obstime'], timezone='UTC')
                dic = arrange.base_data(dict(), data['stno'], data['obstime'], timezone='UTC')
                dic = arrange.wind_arrange(dic, datetime.now(), data['wd'], data['ws'],['即時品管-風力'], ['pass'])
                #wind_qc = arrange.qc_append_arrange(Schema.Wind.key, ['即時品管-風力'], ['pass'])
                dic[Schema.Wind.key][0][Schema.Wind.QC.key] = arrange.qc_arrange(['即時品管-風力'], ['pass'])

            insert_data.append(dic)
            where_data.append({Schema.id:id})
        #result = mongodb.mongo_insert(self.db, self.collection, insert_data)
        result = mongodb.mongo_upsert(self.db, self.collection, where_data, insert_data, upsert=True)        
        print('insert data rows: %s, upsert data rows: %s, update data rows: %s '% (result.inserted_count, result.upserted_count, result.modified_count))

        print(mongodb.mongo_find(self.db, self.collection, where={Schema.id:"C0A52020211231170000"}, column={Schema.Wind.key:1,Schema.Rain.key:1}))
        
    def insert_manysplandid_qc_result(self, parsed_data):
        '''
        10分鐘後新增多彩的qc品管資料。
        輸入資料應改為該次插入的測站、觀測時間、品管方法名稱及結果。
        此處用parsed_data取代測站、觀測時間。
        多彩品管方法名稱及結果寫死作範例。
        '''
        mongodb = Mongo()
        conn = mongodb.mongo_conn(self.conn_str)
        arrange = MongoDataArrange()
        update_datas = []
        where_datas = []
        # 多組QC資料同時parsing 後要插入資料庫
        for data in parsed_data:
            # 僅抓取 stno、obstime 資料， qc 資料寫死假資料。
            id = arrange.get_id(data['stno'], data['obstime'], timezone='UTC')
            dic = dict()
            dic = arrange.qc_arrange1(dic, Schema.Rain.key, ['多彩品管-rain1', '多彩品管-rain2'], ['Q', 'A'],isappend=True)
            dic = arrange.qc_arrange1(dic, Schema.Pressure.key, ['多彩品管-pressure1', '多彩品管-pressure2'], ['Q', 'A'],isappend=True)
            update_datas.append(dic)
            where_datas.append({Schema.id:id})
            
        result = mongodb.mongo_update_append(self.db, self.collection, where_datas, update_datas)
        print("更新 %s 筆資料" %result.modified_count)
        print(mongodb.mongo_find(self.db, self.collection, where={Schema.id:"C0A52020211231170000"}, column={Schema.Wind.key:1,Schema.Rain.key:1}))

        # 需分開插入qc時，可這樣使用，如果同時取得多種QC結果，可使用上方方式插入
        wind_append_datas = []
        for data in parsed_data:
            id = arrange.get_id(data['stno'], data['obstime'], timezone='UTC')
            dic = dict()
            wind_qc = arrange.qc_append_arrange(Schema.Wind.key,['多彩品管-wind1', '多彩品管-wind2'], ['Q', 'A'])            
            
            wind_append_datas.append(wind_qc)
        result = mongodb.mongo_update_append(self.db, self.collection, where_datas, wind_append_datas)
        print("更新 %s 筆資料" %result.modified_count)
        print(mongodb.mongo_find(self.db, self.collection, where={Schema.id:"C0A52020211231170000"}, column={Schema.Wind.key:1,Schema.Rain.key:1}))


    def insert_obsdata_history(self, parse_data):
        mongodb = Mongo()
        conn = mongodb.mongo_conn(self.conn_str)
        arrange = MongoDataArrange()
        insert_datas = []
        where_datas = []
        for data in parse_data:
            dic = dict()
            dic = arrange.pressure_arrange(dic, datetime.now(), data['pressure'], ['即時品管-氣壓'], ['pass'], isappend=True)
            dic = arrange.wind_arrange(dic, datetime.now(), data['wd'], data['ws'], qc_name_list= ['即時品管-風力'], qc_result_list=['pass'], isappend=True)
            dic = arrange.rain_arrange(dic, datetime.now(), data['rain'], data['rh'], ['即時品管-雨量'], ['pass'], isappend=True)
            id = arrange.get_id(data['stno'], data['obstime'])
            insert_datas.append(dic)
            where_datas.append({Schema.id:id})

        result = mongodb.mongo_update_append(self.db, self.collection, where_datas, insert_datas)
        print("更新 %s 筆資料" %result.modified_count)
        print(mongodb.mongo_find(self.db, self.collection, where={Schema.id:"C0A52020211231170000"}, column={Schema.Wind.key:1,Schema.Rain.key:1}))

def pause():
    os.system('pause')
    return 

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

    mongo = Mongo()
    conn = mongo.mongo_conn(conn_str)
    conn[db][mongo_table].drop()

    test_data = []
    with open("sample_data/sample.csv") as csvfile:
        rows = csv.reader(csvfile)
        next(rows)
        error_count = 0
        row_count = 1
        for r in rows:
            #print('row: %s' %count)
            try:
                dic = dict()
                dic['stno'] = r[0]
                dic['obstime'] = datetime.strptime(r[1],"%Y-%m-%dT%H:%M:%S.%fZ")
                #print(r[2])
                dic['pressure'] = float(r[2])
                dic['tx'] = float(r[3])
                dic['wd'] = float(r[4])
                dic['ws'] = float(r[5])
                dic['rain'] = float(r[6])
                dic['rh'] = float(r[7])
                test_data.append(dic)
                row_count = row_count +1
                if row_count > 2000:
                    break
            except:
                error_count = error_count + 1
                continue
        print('error rows: %s ' %error_count)


    obs_proc = ObsProcessSample(conn_str, db, mongo_table)
    # 模擬 資料處理後 插入 pri 資料
    print('模擬 資料處理後 插入 pri 資料')
    obs_proc.insert_obsdata_from_pri(test_data)
    pause()

    # 模擬 10分鐘後抓到多彩品管結果 更新資料庫
    print('模擬 10分鐘後抓到多彩品管結果 更新資料庫')
    obs_proc.insert_manysplandid_qc_result(test_data)
    pause()

    print('模擬 風力以不同頻率傳入資料')
    # 故意用同一個function 強調第一次的資料都可以用同樣的方式插入
    obs_proc.insert_obsdata_from_pri(test_data, round=2)
    pause()
    
    print('模擬 修改觀測資料 新增一組 觀測結果')
    obs_proc.insert_obsdata_history(test_data)
    pause()
    print('模擬 修改資料QC 10分鐘後抓到多彩品管結果 更新資料庫')
    obs_proc.insert_manysplandid_qc_result(test_data)
    pause()

    # 重複使用upsert 插入會導致整筆資料被覆蓋
    print('模擬 資料處理後 插入 pri 資料')
    obs_proc.insert_obsdata_from_pri(test_data)
    pause()


 