#importing required libraries for python and spark execution
import MySQLdb
import numpy as np
import pandas as pd
import datetime as dt
import pymongo
import warnings
import json
import random

warnings.filterwarnings("ignore")

from operator import add
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import when

from pyspark import SparkConf, SparkContext
from math import sin, cos, sqrt, atan2, radians, degrees
from bson import json_util, ObjectId
from pymongo import MongoClient, GEOSPHERE
from bson.son import SON
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestRegressor
from scipy import stats
from sklearn.metrics import r2_score
from sklearn import linear_model
from elasticsearch import Elasticsearch, helpers
from time import sleep

###################################################################################
############# MANUAL FUNCTION TO CALCULATE HAVERSINE DISTANCE ON RDD  #############
###################################################################################
def distance(x):
    from datetime import timedelta
    #caluclating distance between present and the next point
    x[1]['lag_lat'] = x[1].latitude.shift(1)
    x[1]['lag_lat'][0] = 0

    x[1]['lag_long'] = x[1].longitude.shift(1)
    x[1]['lag_long'][0] = 0

    x[1]['lag_time'] = x[1].timestamp.shift(1)
    x[1]['lag_time'][0] = dt.date.today()
    x[1] = x[1][x[1].lag_time.notnull()]
    x[1]['diffLat'] =  x[1]['latitude'] - x[1]['lag_lat']
    x[1]['diffLong'] =  x[1]['longitude'] - x[1]['lag_long']
    x[1]['temp'] = map(lambda z:sin(z / 2)**2, x[1]['diffLat']) 
    x[1]['temp1'] = map(lambda z:cos(z ), x[1]['latitude'])
    x[1]['temp2'] = map(lambda z:cos(z ), x[1]['lag_lat'])
    x[1]['temp3'] = map(lambda z:sin(z/2)**2, x[1]['diffLong'])
    x[1]['temp'] = x[1]['temp'] + x[1]['temp1']*x[1]['temp2']*x[1]['temp3']
    x[1]['temp'] = map(lambda y: 2 * atan2(sqrt(y), sqrt(1 - y)), x[1]['temp'])
    x[1]['distance'] = 6373*x[1]['temp'] #calculating distance using  Haversine formula
    return x
df1=df.map(distance)


###################################################################################
############# USING SPARK STREAMING TO READ DATA FROM A KAFKA TOPIC   #############
###################################################################################
while True:
    
    try:

        #Stream the data from kafka topic           
        conf = (SparkConf()
                            .setAppName("FUllETA12")
                            .set("spark.executor.memory", spark_memory)
                            .set("spark.local.dir", spark_path)
                        .set("spark.executor.cores",spark_cores))
        try:
            sc = SparkContext(conf = conf)
        except:
            continue
        try:
            ssc1 = StreamingContext(sc, 10)
        except:
            continue           
        kafkaParams = {"metadata.broker.list": kafkaIp_port,"auto.offset.reset":"largest"}
        start = 0
        partition = 0
        topic = topic_subscriber
        topicPartion = TopicAndPartition(topic,partition)
        fromOffset = {topicPartion: long(0)}
        kvs = KafkaUtils.createDirectStream(ssc1, [topic],kafkaParams,)



        kvs1 = kvs.map(lambda x: x[1])
        #kvs1 = kvs1.map(result)
        kvs1.foreachRDD(CALL_FUNCTION)
        ssc1.start()
        ssc1.awaitTermination(18000)
    except Exception as e:
        print(e)

###################################################################################
########## FETCHING GEOGRAPHICALLY CLOSER DATA POINTS FROM ELASTICSEACH  ##########
###################################################################################
 results = es2.search(index=index_elac_search+'*', ignore=[400, 404],doc_type = 'geotest', body={
		                                            "size": 1000,
		                                      "query": {
		                                        "filtered": {
		                                          "query": {
		                                            "bool": {
		                                                                  "must": [
		                                                                    {
		                                                                      "match": {
		                                                                        "imei": x[0]  
                                                                                # "imei": "1142365"
		                                                                      }
		                                                                                        }

		                                                                  ]
		                                                                }
		                                          },
		                                          "filter": {
		                                            "geo_distance": {
		                                              "distance": "200m",
		                                              "location": {
		                                                "lat": latitude,
		                                                "lon": longitude
		                                              }
		                                            }
		                                          }
		                                        }
		                                      }
		                                    }
		                                                     