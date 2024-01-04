import pymongo
import datetime

class Mongo:
    def __init__(self) -> None:
        pass

    def mongo_conn(self):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        return client
    
    def mongo_insert(self, db, table, json_data):
        client = self.mongo_conn()
        collection = client[db][table]

        collection.insert_many(json_data)

    def mongo_find(self, db, table, query = {}, column = {'_id':0}):
        client = self.mongo_conn()
        collection = client[db][table]
        
        if ('_id' in column) == False:
            column['_id'] = 0

        # print('query: %s' %type(query))
        # print('column: %s'%type(column))
        datas = collection.find(query,column)
        r = list(datas)
        return r
    def mongo_update(self, db, table, query = {}, update={}, upsert = False):
        client = self.mongo_conn()
        collection = client[db][table]
        
        result = collection.update_many(query, {"$set":update}, upsert=upsert)
        return result
    
    def mongo_upsert(self,db, table, query = {}, update={}, upsert = True):
        client = self.mongo_conn()
        collection = client[db][table]
        res = collection.update_one(query, {"$set": update}, True)
        return res
        

def find_test( mongo, title = "", db='db1', table='obs', query = {}, column = {}):
    datas = mongo.mongo_find(db, table, query, column)
    print("\n%s" %title)
    for d in datas:
        print(d)

def update_test(mongo:Mongo, title = "", db="db1", table = "obs", query = {}, update = {}, upsert = False):
    if upsert == False:
        r = mongo.mongo_update(db, table, query, update)
    else:
        r = mongo.mongo_upsert(db, table, query, update)
    print("\n%s" %title)
    print(r)
    pass

if __name__ == '__main__':
    mongo = Mongo()
    # datas = mongo.mongo_find('db1', 'obs')
    # for d in datas:
    #     print(d)
    db = 'db1'
    table = 'obs'
    #mongo.mongo_insert(db, table, [{	"stid": "466910",	"obstime": "2023-01-01T01:00",	"obs": {		"ws": 12.1,		"rain": 20,		"temp": 27.1	},	"qc": {		"ws": [			"method1",			"method2"		]	}}])
    #find_test(mongo, db, table)

    find_test(mongo, "get all" )
    stid = '466900'
    find_test(mongo, title= "get station: %s" %stid, query= {"stid":stid})
    find_test(mongo, title="get all and select obs data", column={"obs":1})
    find_test(mongo, title="get rain > 10", query={'obs.rain':{"$gt":10}}, column={"obs":1,"stid":1, "obstime":1})

    update_test(mongo, title="update 2023/01/01 1:00 466900  temperature from 30.2 to 19.7", query={"stid":"466900", "obstime":"2023-01-01T01:00"}, update={"qc.temp":19.6})
    d = datetime.datetime.strptime("2023-01-01T01:00", "%Y-%m-%dT%H:%M")
    update_test(mongo, title="update none exist data and insert", 
                query={"stid":"466880", "obstime": d}, 
                update={"stid":"466880","obstime":d,"obs":{"ws":12.1,"rain":7.4,"temp":15.2},"qc":{"ws":["method1","method2"]}},
                upsert=True
                )
    
    update_test(mongo, title="update none exist data and insert", 
                query={"stid":"466880", "obstime": d}, 
                update={"stid":"466880","obstime":d,"obs":{"ws":12.1,"rain":27.4,"temp":15.2},"qc":{"ws":["method1","method2"]}}
                
                )
    
    rs = mongo.mongo_find(db, table, {"obstime": {"$not" : {"$type": "date"}}})

    for r in rs:
        d = datetime.datetime.strptime(r['obstime'], '%Y-%m-%dT%H:%M')
        mongo.mongo_update(db, table, query={"stid": r['stid'], "obstime":r['obstime']}, update={"obstime": d})

