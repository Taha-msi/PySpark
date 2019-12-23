# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 09:46:26 2019

@author: martyngilbertson
"""

from kafka import KafkaConsumer
import json
consumer = KafkaConsumer(
        'sensor_data',
        group_id='my_second_app',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
res={}
for msg in consumer:
    print (msg.value)
    res[msg.key]=msg.value