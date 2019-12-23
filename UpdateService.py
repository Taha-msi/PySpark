# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 08:33:00 2019

@author: martyngilbertson
"""
import pymysql
import pandas as pd
import numpy as np
import time
from datetime import datetime as dt
from pymongo import MongoClient,InsertOne
from pymongo.errors import InvalidOperation

connection={
        'host':'10.1.0.162',
        'port':27017,
        'user':'godson',
        'passw':'godson123',
        'source':'ktpisl'
        }


def temporaryGetSensorList():
    cursor=mydb.devices.find({'activated':1},{'_id':0,'sensor_id':1})
    sensorL=[]
    for i in cursor:
        sensorL.append(i['sensor_id'])
    return tuple(sensorL)
        

def get_db(host,port,passw,source,user):
    client = MongoClient(host,port,maxPoolSize =200,wTimeoutMS=2500)
    client.admin.authenticate(user, passw,source)
    db = client[source]
    return db

def getSensorsDic():
    sensorList=np.array(inputData["sensorID"].unique(),dtype=np.int32).tolist()
    cursor= mydb.devices.find({'sensor_id': {'$in': sensorList}},{'_id': 0,'system_id': 1,'system_name': 1,'sensor_id': 1,'data_points': 1,'sensor_name':1})
    sens=[]
    time=dt.now()
    for i in cursor:
        readColumns=['gatewayID','recordDate','Battery','RSSI','LQI']
        for j in i['data_points']:
            if time<=j['end_delay'] and time>=j['start_delay']:
                j['alarm_delay']=1
            else:
                j['alarm_delay']=0
            readColumns.append(j['point'])
        i['readColumns']=readColumns
        sens.append(i)
    return sens,sensorList

def getMaxDate(sensorL):
    pipe= [
            {'$match': {'sensor_id': {'$in': sensorL}}},
            
            {'$group': {'_id': 0, 'maxDate': {'$max': '$maxTime'}}},
            {'$project': {'_id': 0}}
          ]
    cursor= mydb.sensorData.aggregate(pipeline=pipe)
    dat=[]
    for i in cursor:
        dat.append(i)
    if len(dat)>0:
        return dat[0]['maxDate']
    return str(dt.now()).split(' ')[0]
    
def inserter(sensor,sensorD):
    sId=sensor['sensor_id']
    inputValues=sensorD.loc[:,sensor['readColumns']]
    for sens in sensor['data_points']:
        A=np.zeros((len(inputValues), 4))
        if sens['alarm_delay']==0:
            A[inputValues[sens['point']]<sens['minT'],0]=1
            A[inputValues[sens['point']]>sens['maxT'],1]=1
            A[inputValues[sens['point']]==sens['minT'],0]=-1
            A[inputValues[sens['point']]==sens['maxT'],1]=-1
        A[:,2]=sens['minT']
        A[:,3]=sens['maxT']
        inputValues[sens['point']+'-minV']= A[:,0]
        inputValues[sens['point']+'-maxV']=A[:,1]
        inputValues[sens['point']+'-minT']=A[:,2]
        inputValues[sens['point']+'-maxT']=A[:,3]
        inputValues[sens['point']+'-activeDate']=sens['threshold_active_date']
        inputValues[sens['point']+'-alarmDelay']=sens['alarm_delay']
        inputValues[sens['point']+'-alarmActiveDate']=sens['start_delay']
    n = 10  #chunk row size
    list_df = [inputValues[i:i+n] for i in range(0,inputValues.shape[0],n)]
    for inpVal in list_df:
        inputDics=inpVal.to_dict('records')
        bulk_inserts=[]
        for row in inputDics:
            doc={}
            date= dt.strptime(str(row['recordDate']),"%Y-%m-%d %H:%M:%S")
            row['hour']=date.hour
            row['system_id']=sensor['system_id']
            doc['dataSamples']=row
            doc['sensor_id']=sId
            doc['recordDay']=date.replace(hour=0, minute=0, second=0)
            insert=InsertOne(doc)
            bulk_inserts.append(insert)
        bulkWriter(bulk_inserts)

def bulkWriter(bulk_inserts):
    try:
        bulk_results = mydb.sensorDataLake.bulk_write(bulk_inserts)
        #print(bulk_results.inserted_count)
    except InvalidOperation:
        print("no updates necessary")
    except Exception as e:
        print(str(e))
        
def loadHistoricalData(sensorList):
    
    pipe=[
            {'$match': {'recordDay': dt.today().replace(hour=0, minute=0, second=0, microsecond=0),'sensor_id': {'$in': sensorList}}},# just for current data
            {'$group': {'_id': {"sensor_id":"$sensor_id","recordDay":"$recordDay"},
                        'dataSamples': {'$push': "$dataSamples"},
                        'nSample':{'$sum':1},'minTime':{'$min':'$dataSamples.recordDate'},
                        'maxTime':{'$max':'$dataSamples.recordDate'},
                        'minRSSI':{'$min':'$dataSamples.RSSI'},
                        'maxRSSI':{'$max':'$dataSamples.RSSI'},
                        'minLQI':{'$min':'$dataSamples.LQI'},
                        'maxLQI':{'$max':'$dataSamples.LQI'},
                        'minBattery':{'$min':'$dataSamples.Battery'},
                        'maxBattery':{'$min':'$dataSamples.Battery'}}},
            {'$addFields': {
                        'temp1_measures': {'min':{'$min': '$dataSamples.temp1'},'max':{'$max': '$dataSamples.temp1'},'average': {'$avg': '$dataSamples.temp1'},'std': {'$stdDevPop': '$dataSamples.temp1'},'n-maxV':{'$sum': '$dataSamples.temp1-maxV'},'n-minV':{'$sum': '$dataSamples.temp1-minV'}},
                        'temp2_measures': {'min':{'$min': '$dataSamples.temp2'},'max':{'$max': '$dataSamples.temp2'},'average': {'$avg': '$dataSamples.temp2'},'std': {'$stdDevPop': '$dataSamples.temp2'},'n-maxV':{'$sum': '$dataSamples.temp2-maxV'},'n-minV':{'$sum': '$dataSamples.temp2-minV'}},
                        'temp3_measures': {'min':{'$min': '$dataSamples.temp3'},'max':{'$max': '$dataSamples.temp3'},'average': {'$avg': '$dataSamples.temp3'},'std': {'$stdDevPop': '$dataSamples.temp3'},'n-maxV':{'$sum': '$dataSamples.temp3-maxV'},'n-minV':{'$sum': '$dataSamples.temp3-minV'}},
                        'temp4_measures': {'min':{'$min': '$dataSamples.temp4'},'max':{'$max': '$dataSamples.temp4'},'average': {'$avg': '$dataSamples.temp4'},'std': {'$stdDevPop': '$dataSamples.temp4'},'n-maxV':{'$sum': '$dataSamples.temp4-maxV'},'n-minV':{'$sum': '$dataSamples.temp4-minV'}},
                        'temp5_measures': {'min':{'$min': '$dataSamples.temp5'},'max':{'$max': '$dataSamples.temp5'},'average': {'$avg': '$dataSamples.temp5'},'std': {'$stdDevPop': '$dataSamples.temp5'},'n-maxV':{'$sum': '$dataSamples.temp5-maxV'},'n-minV':{'$sum': '$dataSamples.temp5-minV'}},
                        'loop1_measures': {'min':{'$min': '$dataSamples.loop1'},'max':{'$max': '$dataSamples.loop1'},'average': {'$avg': '$dataSamples.loop1'},'std': {'$stdDevPop': '$dataSamples.loop1'},'n-maxV':{'$sum': '$dataSamples.loop1-maxV'},'n-minV':{'$sum': '$dataSamples.loop1-minV'}},
                        'loop2_measures': {'min':{'$min': '$dataSamples.loop2'},'max':{'$max': '$dataSamples.loop2'},'average': {'$avg': '$dataSamples.loop2'},'std': {'$stdDevPop': '$dataSamples.loop2'},'n-maxV':{'$sum': '$dataSamples.loop2-maxV'},'n-minV':{'$sum': '$dataSamples.loop2-minV'}},
                        'pulse1_measures': {'min':{'$min': '$dataSamples.pulse1'},'max':{'$max': '$dataSamples.pulse1'},'average': {'$avg': '$dataSamples.pulse1'},'std': {'$stdDevPop': '$dataSamples.pulse1'},'n-maxV':{'$sum': '$dataSamples.pulse1-maxV'},'n-minV':{'$sum': '$dataSamples.pulse1-minV'}},
                        'pulse2_measures': {'min':{'$min': '$dataSamples.pulse2'},'max':{'$max': '$dataSamples.pulse2'},'average': {'$avg': '$dataSamples.pulse2'},'std': {'$stdDevPop': '$dataSamples.pulse2'},'n-maxV':{'$sum': '$dataSamples.pulse2-maxV'},'n-minV':{'$sum': '$dataSamples.pulse2-minV'}},
                        'analog1_measures': {'min':{'$min': '$dataSamples.analog1'},'max':{'$max': '$dataSamples.analog1'},'average': {'$avg': '$dataSamples.analog1'},'std': {'$stdDevPop': '$dataSamples.analog1'},'n-maxV':{'$sum': '$dataSamples.analog1-maxV'},'n-minV':{'$sum': '$dataSamples.analog1-minV'}},
                        'analog2_measures': {'min':{'$min': '$dataSamples.analog2'},'max':{'$max': '$dataSamples.analog2'},'average': {'$avg': '$dataSamples.analog2'},'std': {'$stdDevPop': '$dataSamples.analog2'},'n-maxV':{'$sum': '$dataSamples.analog2-maxV'},'n-minV':{'$sum': '$dataSamples.analog2-minV'}}}
            },
            {'$project': {
                        '_id':0,
                        'sensor_id':'$_id.sensor_id',
                        'recordDay':'$_id.recordDay',
                        'dataSamples':1,
                        'nSample':1,
                        'minTime':1,
                        'maxTime':1,
                        'minRSSI':1,
                        'maxRSSI':1,
                        'minLQI':1,
                        'maxLQI':1,
                        'minBattery':1,
                        'maxBattery':1,
                        'temp1': {'$cond': [{'$eq': ['$temp1_measures.average', None]}, '$$REMOVE', '$temp1_measures']},
                        'temp2': {'$cond': [{'$eq': ['$temp2_measures.average', None]}, '$$REMOVE', '$temp2_measures']},
                        'temp3': {'$cond': [{'$eq': ['$temp3_measures.average', None]}, '$$REMOVE', '$temp3_measures']},
                        'temp4': {'$cond': [{'$eq': ['$temp4_measures.average', None]}, '$$REMOVE', '$temp4_measures']},
                        'temp5': {'$cond': [{'$eq': ['$temp5_measures.average', None]}, '$$REMOVE', '$temp5_measures']},
                        'loop1': {'$cond': [{'$eq': ['$loop1_measures.average', None]}, '$$REMOVE', '$loop1_measures']},
                        'loop2': {'$cond': [{'$eq': ['$loop2_measures.average', None]}, '$$REMOVE', '$loop2_measures']},
                        'pulse1': {'$cond': [{'$eq': ['$pulse1_measures.average', None]}, '$$REMOVE', '$pulse1_measures']},
                        'pulse2': {'$cond': [{'$eq': ['$pulse2_measures.average', None]}, '$$REMOVE', '$pulse2_measures']},
                        'analog1': {'$cond': [{'$eq': ['$analog1_measures.average', None]}, '$$REMOVE', '$analog1_measures']},
                        'analog2': {'$cond': [{'$eq': ['$analog2_measures.average', None]}, '$$REMOVE', '$analog2_measures']}}},
            {'$merge': {'into':'sensorData', 'on':['sensor_id','recordDay'],'whenMatched': 'merge'}}
            #{'$merge': {'into':'sensorData'}} # right after creating this collection and before indexing it is applicable.
            ]
    cursor= mydb.sensorDataLake.aggregate(pipeline=pipe,  allowDiskUse= True)

    
def firstLoad(sensors):
    for sensor in sensors:
        sensorData=inputData.loc[inputData["sensorID"]==sensor['sensor_id']]
        inserter(sensor,sensorData)


mydb=get_db(connection['host'],connection['port'],connection['passw'],connection['source'],connection['user'])

while True:
    db = pymysql.connect("10.1.0.38","taha","2gvftwKL")
    initSensorList=temporaryGetSensorList()
    recTime=getMaxDate(initSensorList)
    inputData= pd.read_sql("SELECT A.gateway_id as gatewayID, A.record_time as recordDate, A.sensor_id as sensorID,0 as loop1, 0 as loop2, B.temperature as temp1, 0 as temp2, 0 as temp3,0 as temp4,0 as temp5,0 as pulse1,0 as pulse2,"
                           +"0 as analog1,0 as analog2, A.battery as Battery, A.rssi as RSSI, A.snr as LQI from dataset.datatype_diagnostics as A, dataset.datatype_107 as B "
                           +"where B.diagnostics_row_id=A.row_id and B.sensor_id in "+str(initSensorList)+" and A.record_time > '"+str(recTime)
                           + "' order by A.sensor_id, A.record_time",db)
    inputData=inputData.append(pd.read_sql("SELECT A.gateway_id as gatewayID, A.record_time as recordDate, A.sensor_id as sensorID,0 as loop1, 0 as loop2,  B.reading_temp_cur as temp1, B.reading_temp_min as temp2, B.reading_temp_max as temp3 ,0 as temp4,0 as  temp5,0 as pulse1,0 as pulse2,0 as analog1,0 as analog2, A.battery as Battery, A.rssi as RSSI, A.snr as LQI "
                                           + " from dataset.datatype_diagnostics as A, dataset.datatype_1 as B "
                                           + " where B.diagnostics_row_id=A.row_id and "
                                           + " B.sensor_id in "+str(initSensorList)  +" and A.record_time > '"+str(recTime)
                                           + "' order by A.sensor_id, A.record_time",db),ignore_index=True,sort=False)
    inputData=inputData.append(pd.read_sql("SELECT A.gateway_id as gatewayID, A.record_time as recordDate, A.sensor_id as sensorID, B.reading_loop1 as loop1, B.reading_loop2 as loop2, 0 as temp1, 0 as temp2, 0 as temp3 ,0 as temp4, 0 as temp5, B.reading_pulse1 as pulse1, B.reading_pulse2 as pulse2, 0 as analog1, 0 as analog2, A.battery as Battery, A.rssi as RSSI, A.snr as LQI "
                                           + " from dataset.datatype_diagnostics as A, dataset.datatype_2 as B "
                                           + " where B.diagnostics_row_id=A.row_id and "
                                           + " B.sensor_id in "+str(initSensorList)  +" and A.record_time > '"+str(recTime)
                                           + "' order by A.sensor_id, A.record_time",db),ignore_index=True,sort=False)
    db.close()
    if len(inputData)>0:
        print("we are going to update "+str(len(inputData))+" records!!")
        delResult=mydb.sensorDataLake.delete_many({'recordDay':{'$ne':dt.today().replace(hour=0, minute=0, second=0, microsecond=0)}})
        sensors,sensorList=getSensorsDic()
        firstLoad(sensors)
        loadHistoricalData(sensorList)
    else:
        print("there is no record to update!!")
    time.sleep(30)