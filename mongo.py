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

        r = collection.insert_many(json_data)
        return r

    def mongo_find(self, db, table, where = {}, column = {'_id':0}, print_flag = False):
        client = self.mongo_conn()
        collection = client[db][table]
        
        if ('_id' in column) == False:
            column['_id'] = 0

        if print_flag == True:
            print('where: %s' %where)
            print('column: %s'%column)
        
        datas = collection.find(where,column)
        r = list(datas)
        return r
    def mongo_update(self, db, table, where = {}, update={}, upsert = False):
        client = self.mongo_conn()
        collection = client[db][table]
        
        result = collection.update_many(where, {"$set":update}, upsert=upsert)
        return result
    
    def mongo_upsert(self,db, table, where = {}, update={}, upsert = True):
        client = self.mongo_conn()
        collection = client[db][table]
        res = collection.update_one(where, {"$set": update}, True)
        return res
    
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
        return result
