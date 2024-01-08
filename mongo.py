import pymongo
from pymongo.errors import AutoReconnect
from retry import retry

class Mongo:
    Client = None
    def __init__(self) -> None:
        pass
    
    def __retry_if_auto_reconnect_error(exception):
        return isinstance(exception, AutoReconnect)

    def mongo_conn(self):
        if self.Client != None:
            return self.Client
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.Client = client
        return client
    
    def mongo_insert(self, db, table, json_data):
        client = self.mongo_conn()
        collection = client[db][table]

        r = collection.insert_many(json_data)
        client.close()
        return r

    def mongo_count(self, db, table, where = {}):
        client = self.mongo_conn()
        col = client[db][table]
        result = col.count_documents(where)
        client.close()
        return result
    
    #@retry(retry_on_exception=__retry_if_auto_reconnect_error, stop_max_attempt_number=2, wait_fixed=2000)
    # @retry(AutoReconnect, tries=3, delay=1)
    def mongo_find(self, db, table, where = {}, column = {'_id':0}, print_flag = False, limit= -1, skip=-1):
        client = self.mongo_conn()
        collection = client[db][table]
        
        if ('_id' in column) == False:
            column['_id'] = 0

        if print_flag == True:
            print('where: %s' %where)
            print('column: %s'%column)
        datas = collection.find(where,column)
        if limit != -1:
            datas = datas.limit(limit)
        if skip != -1:
            datas = datas.skip(skip)
        r = list(datas)
        # client.close()
        return r
    
    def mongo_update(self, db, table, where = {}, update={}, upsert = False):
        client = self.mongo_conn()
        collection = client[db][table]
        
        result = collection.update_many(where, {"$set":update}, upsert=upsert)
        client.close()
        return result
    
    def mongo_upsert(self,db, table, where = {}, update={}, upsert = True):
        client = self.mongo_conn()
        collection = client[db][table]
        res = collection.update_one(where, {"$set": update}, True)
        client.close()
        return res
    
    def mongo_update_multiple(self, db, table, where=[], update=[], upsert = True):
        client = self.mongo_conn()
        collection = client[db][table]
        
        if len(where) != len(update):
            raise "where condition length is different than update length."
        
        obj = []
        for z in zip(where, update):            
            obj.append(pymongo.UpdateMany(z[0], {"$set":z[1]}))
            
        result = collection.bulk_write(obj)
        client.close()
        return result
        
    
    def mongo_group(self, db, table, group = {}, statistics = {}, where = {}):
        client = self.mongo_conn()
        collection = client[db][table]

        if group == {} or statistics == {}:
            return []
        group = {"_id": group}
        merge = {**group, **statistics}        
        group = {"$group": merge}
        match = {"$match":where}

        # print("group: %s\nmatch: %s" %(group, match))

        result = collection.aggregate([match, group])
        client.close()
        return result
