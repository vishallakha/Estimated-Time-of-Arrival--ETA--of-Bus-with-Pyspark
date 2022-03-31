## Description

I have built a pyspark application which reads the current location and speed of a vehicle from an IVC (in-vehicle computer) and calculates the ETA using haversine distance formula which is stored with every trip during training time. Then during streaming, the ETA is calculated as the average of past values from ElasticSearch. The final predictions are stored to a kafka topic for the front end user.


## Technology Stack
Python, Pyspark, Pyspark Streaming, Kafka, HDFS ,Elasticsearch