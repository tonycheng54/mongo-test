import os
import datetime
from lib.mongo import Mongo
from pymongo import UpdateOne
import time
def convert_timestring_to_timeformat(mongo:Mongo,db="db1", table="obs"):
    
    rs = mongo.mongo_find(db, table, {"ObsTime": {"$not" : {"$type": "date"}}})
    print(len(rs))
    os.system("pause")

    for r in rs:
        print(r['ObsTime'])
        d = datetime.datetime.strptime(r['ObsTime'], '%Y-%m-%dT%H:%M:%S')
        mongo.mongo_update(db, table, where={"Stno": r['Stno'], "ObsTime":r['ObsTime']}, update={"ObsTime": d})
        
def convert_timeformat(db='db1', table=''):
    mongo = Mongo()
    mongo.mongo_conn("localhost:27017,localhost37017")
    #db = conn[db]
    #col = db[table]
    total_rows = mongo.mongo_count(db, table,{"ObsTime": {"$not" : {"$type": "date"}}})
    print('total rows: %s' % total_rows)
    #total_rows = 100
    limit = 10000
    mongo_cost = 0
    mongo_cost_list = []
    start = time.time()
    for i in range(0, total_rows, limit):
        offset = i
        #mongo.mongo_find({"ObsTime": {"$not" : {"$type": "date"}}})
        docs = mongo.mongo_find(db, table, where={"ObsTime": {"$not" : {"$type": "date"}}}, limit= limit)
        # if len(list(docs)) == 0:
        #     print('limit: %s skip: %s' %(limit, offset))
        # else:
            
        where_list = []
        update_list = []
        for doc in docs:
            d = datetime.datetime.strptime(doc['ObsTime'], '%Y-%m-%dT%H:%M:%S')
            where_list.append({"Stno": doc['Stno'], "ObsTime":doc['ObsTime']})
            update_list.append({"ObsTime":d})
            
        mongo_start = time.time()
        result = mongo.mongo_update_multiple(db, table, where= where_list, update= update_list)
        mongo_end = time.time()
        mongo_cost += (mongo_end - mongo_start)
        mongo_cost_list.append(mongo_end - mongo_start)
        
    end = time.time()
    print("total cost: %s sec (%s min)" %((end - start), (end-start) / 60))
    print("mongo cost: %s sec (%s min)" %((mongo_cost), (mongo_cost) / 60))
    print("each update cost is (ms)")
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
            
        
    return total_rows
            

 
if __name__ == "__main__":
    
    #table_name_list = ['cwbhour', 'autoprechour', 'agrhour']
    table_name_list = ['cwbhour'] #, 'agrhour', 'autoprechour']
    try:
        
        for table in table_name_list:
            start = time.time()
            total_rows = convert_timeformat(table= table)
            end = time.time()
            print("### ")
            print(" %s convert Obstime,  total rows: %s, spent: %s sec" % (table, total_rows, (end - start)))
    except:
        end  = time.time()
        print("exception: time spent: %s sec (%s min)"% ((end - start), (end - start)/60))
    
    # for table in table_name_list:
    #     convert_timestring_to_timeformat(mongo, table=table)
    
    # conn = mongo.mongo_conn()
    # table = 'cwbhour'
    # total_rows = conn['db1'][table].count_documents({"ObsTime": {"$not" : {"$type": "date"}}})
    # print(total_rows)
    # total_rows = 100
    # limit = 10
    
    # col = conn['db1'][table]
    # col.bulk_write(
    #     [
    #         UpdateOne
    #     ]
    # )
    # bulk =  conn['db1'].items.initializeUnorderedBulkOp()
    # for i in range(0, total_rows, limit):
    #     ofset = i
        
    # bulk.execute();
    
        