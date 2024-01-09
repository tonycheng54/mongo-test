from lib.mongo import Mongo
import datetime
import os


def compose_statistic(field, ope, output_field):
    result = {}
    for d in zip(field, ope, output_field):
        result[d[2]] = {d[1]: "$%s"% d[0]}

    return result


def find_test( mongo, title = "", db='db1', table='obs', query = {}, column = {}):
    datas = mongo.mongo_find(db, table, query, column)
    if title != "":
        print("\n%s" %title)
    for d in datas:
        print(d)

def get_extreme_attribute(db, table):
    start_time = datetime.datetime(2023,1,1,0,0)
    end_time = datetime.datetime(2023,1,2,0,0)
    match = {"obstime":{"$gte":start_time, "lt":end_time}}
    group = {"stid":"$stid"}



if __name__ == '__main__':
    mongo = Mongo()
    db = 'db1'
    table = 'obs'
    
    statistic_start = datetime.datetime(2023, 1, 1, 0, 0)
    statistic_end =   datetime.datetime(2023, 1, 2, 0, 0)
    find_test(mongo, title="print all data", db= db, table=table)
    print("\n")
    
    field = ["obs.rain", "obs.ws", "obs.temp", "obs.temp"]
    ope = ["$sum", "$max", "$max", "$min"]
    output_field = ["rain", "ws", "max_temp", "min_temp"]
    
    match = {"obstime":{"$gte":statistic_start, "$lt": statistic_end}}
    group = {"stid":"$stid"}
    #statistic = {"rain":{"$sum":"$obs.rain"}, "ws":{"$max":"$obs.ws"}, "temp":{"$min":"$obs.temp"}}
    statistic = compose_statistic(field=field, ope=ope, output_field=output_field)

    # print("group: %s" %group)
    # print("statistic: %s" %statistic)
    # print("match:%s" %match)
    
    result = mongo.mongo_group(db, table, group = group, statistics = statistic, where = match)
    obsday_datas = []
    for r in result:
        print(r)
        stid = r["_id"]['stid']
        print("station: %s" %stid)
        print("rain: %s  ws: %s, min_temp: %s, max_temp: %s" %(r['rain'], r['ws'], r['min_temp'], r['max_temp']))
        rain_occur_time = mongo.mongo_find(db, table, {"stid": stid, "rain": r['rain'], "obstime":{"$gte":statistic_start, "$lt": statistic_end}}, {"obstime":1})
        wd_occur_time = mongo.mongo_find(db, table, {"stid": stid, 'obs.ws':r['ws'], "obstime":{"$gte":statistic_start, "$lt": statistic_end}}, {'obstime':1, 'obs.wd':1}, print_flag=True)
        
        print(wd_occur_time)
        os.system("pause")
        min_temp_occur_time = mongo.mongo_find(db, table, {"stid": stid, 'obs.temp':r['min_temp'], "obstime":{"$gte":statistic_start, "$lt": statistic_end}}, {'obstime':1,})
        max_temp_occur_time = mongo.mongo_find(db, table, {"stid": stid, 'obs.temp':r['max_temp'], "obstime":{"$gte":statistic_start, "$lt": statistic_end}}, {'obstime':1,})
        data = {
            "stid":stid,
            "obstime":datetime.datetime(statistic_start.year, statistic_start.month, statistic_start.day),
            "obs":{
                "ws": r['ws'],
                "wd": wd_occur_time[0]['obs']['wd'] if 'wd' in  wd_occur_time[0]['obs'] else None,
                "rain": r['rain'],
                "max_temp": r['max_temp'],
                "max_temp_occtime": min_temp_occur_time[0]
            }
        }
        obsday_datas.append(data)

    
    table = "obsday"
    mongo.mongo_insert(db, table, obsday_datas)

    