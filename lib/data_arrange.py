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
    
    def __arrange_append_array_format(self, append_data, position):
        return {'$each':[append_data], '$position':position}
    
    def get_id(self, stno, obstime, timezone= 'Asia/Taipei'):
        return  stno + obstime.strftime('%Y%m%d%H%M%S')

    def base_data(self, dic, stno, obstime, timezone='Asia/Taipei'):
        dic = dict()
        if obstime is None or stno is None:
            raise "missing stno or obstime"
        dic[Schema.id] = self.get_id(stno, obstime, timezone)
        dic[Schema.stationid] = stno
        dic[Schema.obstime] = self.__time_format(obstime, timezone)
        #dic[Schema.obstime] = datetime.datetime.strptime(obstime, timeformat)
        return dic
    
    def pressure_arrange(self, dic:dict,  updatetime, pressure, qc_name_list:list=None, qc_result_list:list=None, isappend=False):
        data = dict()
        data[Schema.Pressure.updatetime] = self.__set_updatetime(updatetime)
        data[Schema.Pressure.pressure] = pressure
        if qc_name_list != None and qc_result_list != None:
            data[Schema.Pressure.QC.key] = self.qc_arrange(qc_name_list, qc_result_list)

        if isappend == False:
            dic[Schema.Pressure.key] = []
            dic[Schema.Pressure.key].append(data)
        else:
            #d = {'$each':[data], '$position':0}
            dic[Schema.Pressure.key] = self.__arrange_append_array_format(data, 0)
        return dic

        # data = dict()
        # data[Schema.Pressure.updatetime] = self.__set_updatetime(updatetime)
        # data[Schema.Pressure.pressure] = pressure
        # #data[Schema.Pressure.qc_key] = self.qc_arrange( ['cond'], ['success'])
        
        # if isappend == False:
        #     dic[Schema.Pressure.key].append(data)
        # else:
        #     dic[Schema.Pressure.key] = data
        # return dic
    
    def tx_arrange(self, dic: dict, updatetime, tx, qc_name_list:list=None, qc_result_list:list=None,  isappend=False):

        data = dict()
        data[Schema.Tx.updatetime] = self.__set_updatetime(updatetime=updatetime)
        data[Schema.Tx.tx] = tx
        if isappend == False:
            dic[Schema.Tx.key] = []
            dic[Schema.Tx.key].append(data)
        else:
            dic[Schema.Tx.key] = self.__arrange_append_array_format(data, 0)
        return dic
        
    def wind_arrange(self, dic: dict, updatetime, wd, ws, wg = None, qc_name_list:list=None, qc_result_list:list=None, isappend=False):
        
        data = dict()
        data[Schema.Wind.updatetime] = self.__set_updatetime(updatetime)
        data[Schema.Wind.wd] = wd
        data[Schema.Wind.ws] = ws
        if wg is not None:
            data[Schema.Wind.wg] = wg
        if isappend == False:
            dic[Schema.Wind.key] = []
            dic[Schema.Wind.key].append(data)
        else:
            dic[Schema.Wind.key] = self.__arrange_append_array_format(data, 0)
        return dic
    
    def rain_arrange(self, dic: dict, updatetime, precp, rh = None, qc_name_list:list=None, qc_result_list:list=None, isappend=False):        
        data = dict()
        data[Schema.Wind.updatetime] = self.__set_updatetime(updatetime)
        data[Schema.Rain.precp] = precp
        if rh is not None:
            data[Schema.Rain.rh] = rh

        if isappend == False:
            dic[Schema.Rain.key] = []
            dic[Schema.Rain.key].append(data)
        else:
            dic[Schema.Rain.key] = self.__arrange_append_array_format(data, 0)
        return dic

    def qc_arrange(self, qc_name_list:list, qc_result_list:list):
        if len(qc_name_list) != len(qc_result_list):
            raise "name size is diffrent than result"
        data = []
        for combine in zip(qc_name_list, qc_result_list):
            dic = dict()
            dic[Schema.AttributeBase.QC.name] = combine[0]
            dic[Schema.AttributeBase.QC.result] = combine[1]
            data.append(dic)
        return data
    
    def qc_arrange1(self, dic, obs_key, qc_name_list, qc_result_list, isappend=False):
        '''
        產製QC格式
        '''
        if len(qc_name_list) != len(qc_result_list):
            raise "name size is diffrent than result"
        
        data = []
        for combine in zip(qc_name_list, qc_result_list):
            d = dict()
            d[Schema.AttributeBase.QC.key] = combine[0]
            d[Schema.AttributeBase.QC.result] = combine[1]
            data.append(d)

        if isappend == False:
            print(dic[obs_key])
            dic[obs_key][0][Schema.AttributeBase.QC.key] = data
        else:
            key = "%s.0.%s"%(obs_key,Schema.AttributeBase.QC.key)
            dic[key] = {"$each":data}

        # data = []
        # for combine in zip(qc_name_list, qc_result_list):
        #     dic = dict()
        #     dic[Schema.QC.name] = combine[0]
        #     dic[Schema.QC.result] = combine[1]
        #     data.append(dic)
        # print(data)
        # if isappend == False:
        #     dic[obs_key][0][Schema.__AttributeBase.qc_key] = data
        # else:
        #     print(dic)
        #     key = "%s.0.%s"%(obs_key,Schema.Rain.qc_key)
        #     dic[key] = {"$each":data}
        return dic
        #dic[Schema.Pressure.key][0][Schema.Pressure.qc_key] = arrange.qc_arrange(['即時品管-氣壓'], ['pass'])

    def qc_append_arrange(self, obs_key, qc_name_list, qc_result_list):
        '''
        嘗試與qc_arrange合併
        '''
        dic = dict()
        key = "%s.0.%s"%(obs_key,Schema.AttributeBase.QC.key)
        if len(qc_name_list) != len(qc_result_list):
            raise "name size is diffrent than result"
        qcdata = self.qc_arrange(qc_name_list= qc_name_list, qc_result_list=qc_result_list)
        dic[key] = {"$each":qcdata}
        return dic