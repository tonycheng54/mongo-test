import pymongo

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