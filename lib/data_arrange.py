import datetime
import pytz
from lib.mongoschema import Schema
import os


class MongoDataArrange:
    def __init__(self) -> None:
        pass

    def __set_updatetime1(self, dic, updatetime):
        dic = updatetime if updatetime is not None else datetime.datetime.now()
        return dic
        #dic = datetime.datetime.strptime(updatetime, '%Y-%m-%d %H:%M:%S') if updatetime is not None else datetime.datetime.now()
        #return updatetime if updatetime is not None else datetime.datetime.now()
    
    def __time_format(self, time, timezone):
        if time is None:
            return pytz.timezone(timezone).localize(datetime.datetime.now())
        elif isinstance(time, str):
            return pytz.timezone(timezone).localize(datetime.datetime.fromisoformat(time))
        else:
            return pytz.timezone(timezone).localize(time)

    def __set_updatetime(self, updatetime:datetime= None, timezone='Asia/Taipei'):
        '''
        之後可以用__time_format取代
        '''
        
        return self.__time_format(updatetime, timezone)

    def base_data(self, dic, stno, obstime, timezone='Asia/Taipei'):
        dic = dict()
        if obstime is None or stno is None:
            raise "missing stno or obstime"
        dic[Schema.id] = stno + obstime.strftime('%Y%m%d%H%M%S')
        dic[Schema.stationid] = stno
        dic[Schema.obstime] = self.__time_format(obstime, timezone)
        #dic[Schema.obstime] = datetime.datetime.strptime(obstime, timeformat)
        return dic
    
    def pressure_arrange(self, dic:dict, pressure, updatetime = None):

        dic[Schema.Pressure.key] = []
        data = dict()
        data[Schema.Pressure.updatetime] = self.__set_updatetime(updatetime)
        data[Schema.Pressure.pressure] = pressure
        data[Schema.Pressure.qc_key] = dict()
        data[Schema.Pressure.qc_key][Schema.Pressure.QC.qc_name] = 'cond'
        data[Schema.Pressure.qc_key][Schema.Pressure.QC.result] = 'result'

        dic[Schema.Pressure.key].append(data)
        return dic
    
    def tx_arrange(self, dic: dict, tx, updatetime = None):
        dic[Schema.Tx.key] = []
        data = dict()
        data[Schema.Tx.updatetime] = self.__set_updatetime(updatetime=updatetime)
        data[Schema.Tx.tx] = tx
        dic[Schema.Tx.key].append(data)
        return dic
    
    def wind_arragen(self, dic: dict, wd, ws, wg = None, updatetime = None):
        dic[Schema.Wind.key] = []
        data = dict()
        data[Schema.Wind.updatetime] = self.__set_updatetime(updatetime)
        data[Schema.Wind.wd] = wd
        data[Schema.Wind.ws] = ws
        if wg is not None:
            data[Schema.Wind.wg] = wg
        dic[Schema.Wind.key].append(data)
        return dic
    
    def rain_arrange(self, dic: dict, precp, rh = None, updatetime = None):
        dic[Schema.Rain.key] = []
        data = dict()
        data[Schema.Wind.updatetime] = self.__set_updatetime(updatetime)
        data[Schema.Rain.precp] = precp
        if rh is not None:
            data[Schema.Rain.rh] = rh
        dic[Schema.Rain.key].append(data)
        return dic
        

