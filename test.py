from mongo import Mongo
import datetime
import os        

def find_test( mongo, title = "", db='db1', table='obs', query = {}, column = {}):
    datas = mongo.mongo_find(db, table, query, column)
    if title != "":
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
    

def insert_test(mongo:Mongo, title = "", db="db1", table = "obs", insert_data = []):
    r = mongo.mongo_insert(db, table, insert_data)
    print("\n%s" %title)
    print(r)

def convert_timestring_to_timeformat(mongo:Mongo,db="db1", table="obs"):
    
    rs = mongo.mongo_find(db, table, {"obstime": {"$not" : {"$type": "date"}}})

    for r in rs:
        d = datetime.datetime.strptime(r['obstime'], '%Y-%m-%dT%H:%M')
        mongo.mongo_update(db, table, query={"stid": r['stid'], "obstime":r['obstime']}, update={"obstime": d})

if __name__ == '__main__':
    mongo = Mongo()
    # datas = mongo.mongo_find('db1', 'obs')
    # for d in datas:
    #     print(d)
    db = 'db1'
    table = 'obs'
    conn = mongo.mongo_conn()
    conn[db][table].drop()
    

    timeformat = "%Y-%m-%d %H:%M"
    d1 = datetime.datetime.strptime("2023-01-01 01:00", timeformat)
    d2 = datetime.datetime.strptime("2023-01-01 02:00", timeformat)
    d3 = datetime.datetime.strptime("2023-01-01 03:00", timeformat)
    insert_data = [{"stid":"466900","obstime":d1, "obs":{"ws":12,"wd":240,"rain":37,"temp":30.2},"qc":{"rain":["method1","method2"],"ws":["method3"],"wd":["method1"]}},
                   {"stid":"466910","obstime":d1, "obs":{"ws":12.1,"rain":20,"temp":27.1},"qc":{"ws":["method1","method2"]}},
                   {"stid":"466900","obstime":d2,"obs":{"ws":17,"wd":230,"rain":11},"qc":{"rain":["method1","method3"]}},
                   {"stid":"466900","obstime":d3,"obs":{"rain":2, "temp":11},"qc":{"temp":["method1","method2"]}}
                   ]
    insert_test(mongo, title="insert test data",insert_data=insert_data)

    
    
    find_test(mongo, "get all" )
    stid = '466900'
    find_test(mongo, title= "get station: %s" %stid, query= {"stid":stid})
    find_test(mongo, title="get all and select obs data", column={"obs":1})
    find_test(mongo, title="get rain > 10", query={'obs.rain':{"$gt":10}}, column={"obs":1,"stid":1, "obstime":1})
    #os.system("pause")
    update_test(mongo, title="update 2023-01-01 01:00 466900  temperature from 30.2 to 19.7", query={"stid":"466900", "obstime":d1}, update={"obs.temp":19.6})
    find_test(mongo,query={"stid":"466900", "obstime":d1})
    #os.system("pause")
    update_test(mongo, title="update 2023-01-01 02:00 466900 add temperature 18.1", query={"stid":"466900", "obstime": d2}, update={"obs.temp":18.1})
    find_test(mongo, query={"stid":"466900", "obstime": d2})
    #os.system("pause")

    update_test(mongo, title="update none exist data and insert", 
                query={"stid":"466880", "obstime": d1}, 
                update={"stid":"466880","obstime":d1,"obs":{"ws":12.1,"rain":7.4,"temp":15.2},"qc":{"ws":["method1","method2"]}},
                upsert=True
                )
    find_test(mongo, query={"stid":"466880", "obstime": d1})
    #os.system("pause")
    
    
    find_test(mongo, title="get obstime > 2023-01-01 01:00", query={"obstime":{"$gt":datetime.datetime(2023, 1, 1, 1, 0, 0)}})
    
    table = "obsday"
    statistic_start = datetime.datetime.strptime('2023-01-01 00:00', timeformat)
    statistic_end =   datetime.datetime.strptime('2023-01-02 00:00', timeformat)
    match = {"obstime":{"$gte":statistic_start, "$lt": statistic_end}}

    group = {"_id":{"stid":"$stid"}}
    statistic = {"rain":{"$sum":"$obs.rain"}, "ws":{"$max":"obs.ws"}}
    print("group: %s" %group)
    print("statistic: %s" %statistic)
    print("match:%s" %match)
    #os.system("pause")
    result = mongo.mongo_group(db, table, group = group, statistics = statistic, where = match)
    for r in result:
        print(r)