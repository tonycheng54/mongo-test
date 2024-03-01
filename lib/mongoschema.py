class Schema:
    # class QC:
    #     key = 'QC'
    #     qc_name = "name"
    #     result = "value"

    class AttributeBase():
        updatetime = "updatetime"
        #qc_key = "QC"
        
        class QC:
            key = "QC"
            name = "name"
            result = "value"

    id = '_id'
    stationid = "Stno"
    obstime = "ObsTime"
    updatetime = "updatetime"

    class Pressure(AttributeBase):
        #updatetime = "updatetime"
        key = "StnPres"
        pressure  = "pressure"

    class Tx(AttributeBase):
        #updatetime = "updatetime"
        key = 'Tx'
        tx = "Tx"

    class Wind(AttributeBase):
        #updatetime = "updatetime"
        key = 'Wind'
        ws = 'WS'
        wd = 'WD'
        wg = 'WG'
    class Rain(AttributeBase):
        #updatetime = 'updatetime'
        key = 'Rain'
        precp = 'Precp'
        rh = 'RH'
