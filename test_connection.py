from lib.mongo import Mongo
import time

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
                #mongodb.mongo_insert(db, mongo_table, obsdatas)
                mongodb.mongo_update_multiple(db, mongo_table, ids, obsdatas)

                round_end = time.time()
                info = "%s" % (data['ObsTime'])
                informations.append(info)
                action_rows.append(len(datas))

                round_history.append((round_end - round_start) * 1000)
                show('insert_process.csv', "cost: %s,    insertrows: %s,    obstime: %s" %((round_end - round_start) * 1000, len(datas), data['ObsTime']))
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


if __name__ == '__main__':