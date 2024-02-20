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
    host = None
    replicaName = None
    def __init__(self) -> None:
        pass

    def mongo_conn(self, uri='localhost', port='27017', replicaName = None):        
        return self.mongo_conn('%s:%s' %(uri, port,replicaName))
    
    def mongo_conn(self, host=None, replicaName = None):
        if self.Client != None:
            return self.Client
        if host == None and self.host == None:
            raise "connect string fail"
        if host != None:
            self.host = host

        conn_str = "mongodb://%s/?readPreference=secondaryPreferred"%(self.host)
        if replicaName != None:
            conn_str = "%s&replicaSet=%s" %(conn_str, replicaName)
            self.replicaName = replicaName
        elif self.replicaName != None: 
            conn_str = "%s&replicaSet=%s" %(conn_str, self.replicaName)
        print(conn_str)
        #url = 'mongodb://localhost:27017,localhost:37017,localhost:47017/?readPreference=secondaryPreferred'
        client = pymongo.MongoClient(conn_str)
        
        self.Client = client
        self.host = host
        self.replicaName = replicaName
        return client
    
    def mongo_close(self, conn: pymongo.MongoClient):
        conn.close()
        self.Client = None
    
    def mongo_insert(self, db, table, json_data:list, conn_close=False):
        client = self.mongo_conn()
        collection = client[db][table]
        r = collection.insert_many(json_data)
        if conn_close == True:
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
    
    def mongo_update(self, db, table, where = {}, update={}, upsert = False, conn_close=False):
        client = self.mongo_conn()
        collection = client[db][table]
        
        result = collection.update_many(where, {"$set":update}, upsert=upsert)
        if conn_close == True:
            self.mongo_close(client)
        return result
    
    def mongo_upsert(self,db, table, where = {}, update={}, upsert = True, conn_close=False):
        client = self.mongo_conn()
        collection = client[db][table]
        res = collection.update_one(where, {"$set": update}, upsert= upsert)
        if conn_close == True:
            self.mongo_close(client)
        return res
    
    def __mongo_update_multiple(self, db, table, where=[], update=[], operation='$set', upsert= True, conn_close=False):
        client = self.mongo_conn()
        collection = client[db][table]
        
        if len(where) != len(update):
            raise "where condition length is different than update length."
        
        obj = []
        for z in zip(where, update):   
            obj.append(pymongo.UpdateMany(z[0], {operation:z[1]}, upsert=upsert))
        #result = collection.bulk_write(pymongo.UpdateMany(where, {"$set":update}, upsert=upsert))

        result = collection.bulk_write(obj)
        if conn_close == True:
            self.mongo_close(client)
        return result
    
    def mongo_update_multiple(self, db, table, where=[], update=[], upsert = True, conn_close=False):
        return self.__mongo_update_multiple(db, table, where, update, '$set', upsert, conn_close )

        # client = self.mongo_conn()
        # collection = client[db][table]
        
        # if len(where) != len(update):
        #     print("where condition length(%s) is different than update length(%s)." %(len(where), len(update)))
        #     raise "where condition length(%s) is different than update length(%s)." %(len(where), len(update))
        
        # obj = []
        # for z in zip(where, update):   
        #     obj.append(pymongo.UpdateMany(z[0], {"$set":z[1]}, upsert=upsert))
        # #result = collection.bulk_write(pymongo.UpdateMany(where, {"$set":update}, upsert=upsert))

        # result = collection.bulk_write(obj)
        # self.mongo_close(client)
        # return result
    
    def mongo_update_append(self, db, table, where=[], update=[], conn_close=False):
        return self.__mongo_update_multiple(db, table, where, update, '$push', upsert=False, conn_close=False)
    
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
    client = None
    def __init__(self, client) -> None:
        self.client = client
        pass
    
    def create_index(self, db, table_name, index:[], unique=False):
        m = Mongo()
        client = self.client
        col = client[db][table_name]
        col.create_index(index, unique = unique)


class MongoAdmin:
    def __init__(self) -> None:
        pass
    def create_user(self, user_name, passwd, db:[], collection:[]):
        pass