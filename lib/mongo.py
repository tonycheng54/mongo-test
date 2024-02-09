import pymongo
from pymongo.errors import AutoReconnect


class Opretor:
    gte = "$gte"
    gt = "$gt"
    lt = "$lt"
    lte  = "$lte"
    In = "$in"
    max = "$max"
    min = "$min"

class Function:
    CurrentDate = "$currentDate"

class Mongo:
    Client = None
    def __init__(self) -> None:
        pass

    def mongo_conn(self, uri='localhost', port='27017'):        
        return self.mongo_conn('%s:%s' %(uri, port))
    
    def mongo_conn(self, host='localhost:27017'):
        if self.Client != None:
            return self.Client
        #url = 'mongodb://localhost:27017,localhost:37017,localhost:47017/?readPreference=secondaryPreferred'
        client = pymongo.MongoClient("mongodb://%s/?readPreference=secondaryPreferred" %(host))
        self.Client = client
        return client
    
    def mongo_close(self, conn: pymongo.MongoClient):
        conn.close()
        self.Client = None
    
    def mongo_insert(self, db, table, json_data):
        client = self.mongo_conn()
        collection = client[db][table]
        r = collection.insert_many(json_data)
        self.mongo_close(client)
        return r

    def mongo_count(self, db, table, where = {}):
        client = self.mongo_conn()
        col = client[db][table]
        result = col.count_documents(where)
        self.mongo_close(client)
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
        self.mongo_close(client)
        return result
    
    def mongo_upsert(self,db, table, where = {}, update={}, upsert = True):
        client = self.mongo_conn()
        collection = client[db][table]
        res = collection.update_one(where, {"$set": update}, upsert= upsert)
        self.mongo_close(client)
        return res
    
    def mongo_update_multiple(self, db, table, where=[], update=[], upsert = True):
        client = self.mongo_conn()
        collection = client[db][table]
        
        if len(where) != len(update):
            raise "where condition length is different than update length."
        
        obj = []
        for z in zip(where, update):   
            obj.append(pymongo.UpdateMany(z[0], {"$set":z[1]}, upsert=upsert))
            
        result = collection.bulk_write(obj)
        self.mongo_close(client)
        return result
    def mongo_update_append(self, db, table, where=[], update=[]):
        pass
        
    
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
        self.mongo_close(client)
        return result

class MongoSchema:
    def __init__(self) -> None:
        pass
    
    def create_index(self, db, table_name, index:[], unique=False):
        m = Mongo()
        client = m.mongo_conn()
        col = client[db][table_name]
        col.create_index(index, unique = unique)


class MongoAdmin:
    def __init__(self) -> None:
        pass
    def create_user(self, user_name, passwd, db:[], collection:[]):
        pass