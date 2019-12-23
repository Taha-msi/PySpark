# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 14:39:54 2019

@author: martyngilbertson
"""

import os
import sys
import numpy as np
import pymysql
import pandas as pd
import time
from datetime import datetime as dt
from pymongo import MongoClient,InsertOne
from pymongo.errors import InvalidOperation
from pyspark.sql import SparkSession


connection={
        'host':'10.1.0.162',
        'port':27017,
        'user':'godson',
        'passw':'godson123',
        'source':'ktpisl'
        }

sensorList=(6316733,6325748,6325750,6325751,6325752,6325753,6325754,6325755,6325757,6325758)

def get_db(host,port,passw,source,user):
    client = MongoClient(host,port,maxPoolSize =200,wTimeoutMS=2500)
    client.admin.authenticate(user, passw,source)
    db = client[source]
    return db

def getSensorsDic():
    # It is done based on spark
    sensorList=list(map(float,inputData.select("sensorID").distinct().toPandas()["sensorID"].values.tolist()))
    cursor= mydb.devices.find({'sensor_id': {'$in': sensorList}},{'_id': 0,'system_id': 1,'system_name': 1,'sensor_id': 1,'data_points': 1,'sensor_name':1})
    sens=[]
    time=dt.now()
    for i in cursor:
        readColumns=['recordDate','Battery','RSSI','LQI']
        for j in i['data_points']:
            if time<=j['end_delay']:
                j['alarm_delay']=1
            else:
                j['alarm_delay']=0
            readColumns.append(j['lable'])
        i['readColumns']=readColumns
        sens.append(i)
    return sens

os.chdir("C:/Users/tahamansouri/Documents/KTPISL")
#Where you installed spark.    
os.environ['SPARK_HOME'] = 'C:/Users/tahamansouri/spark-2.4.4-bin-hadoop2.7'
os.curdir
# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']
#Add the following paths to the system path. Please check your installation
#to make sure that these zip files actually exist. The names might change
#as versions change.
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.1-src.zip"))

#Initialize SparkSession and SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("V2 Maestros") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","4") \
    .config("spark.sql.warehouse.dir", "file:///c:/temp/spark-warehouse")\
    .getOrCreate()
SpContext = SpSession.sparkContext

#Import data


Wow that workssssssssssssssssssssss

inputData = SpSession.read.csv("raw-2019-11-05.csv",header=True)
inputData.show()
mydb=get_db(connection['host'],connection['port'],connection['passw'],connection['source'],connection['user'])
sensors=getSensorsDic()
from pyspark.sql import Row
sensors_1=SpContext.parallelize(sensors)
sensors_2= sensors_1.map(lambda p: Row(data_points=p['data_points'],readColumns=p['readColumns'],sensor_id=p['sensor_id']))

sensors_3=SpSession.createDataFrame(sensors_2)

sensors_4=sensors_3.toPandas()



