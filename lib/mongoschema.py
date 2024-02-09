

    


class Schema:
    class __AttributeBase:
        updatetime = "updatetime"
        qc_key = "QC"
        
        class QC:
            qc_name = "name"
            result = "value"

    id = '_id'
    stationid = "Stno"
    obstime = "ObsTime"
    updatetime = "updatetime"

    class Pressure(__AttributeBase):
        #updatetime = "updatetime"
        key = "StnPres"
        pressure  = "pressure"

    class Tx(__AttributeBase):
        #updatetime = "updatetime"
        key = 'Tx'
        tx = "Tx"

    class Wind(__AttributeBase):
        #updatetime = "updatetime"
        key = 'Wind'
        ws = 'WS'
        wd = 'WD'
        wg = 'WG'
    class Rain(__AttributeBase):
        #updatetime = 'updatetime'
        key = 'Rain'
        precp = 'Precp'
        rh = 'RH'
