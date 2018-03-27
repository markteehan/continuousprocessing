# continuousprocessing
Scripts for a CP stack using HANA, Spark Structured Streaming, kafka, HANA Streaming Analytics, Solr, Prometheus and Grafana
The idea is to read a table definition from a database; and then create all of the scripts to start a continuous processing stream 
for data for this table. The source of the stream is usually CSV files; but it can be any any source compatible with the stream-sending 
product (in this case SAP HANA Streaming Analytics (formerly Sybase ESP).


Scripts to automate addition and configuration of new continuous processing streams using shell scripts, python and a repository database.
It autogenerates:
1. a streaming script (xx.CCL) for HANA streaming Analytics which streams new CSV files from an on-prem drop-directory
   1.a Any data attribute can be tokenized to an irreversible token using hash algorithms+noise
   1.b The token vault is located on-prem/in-country, and the streaming service retains a cache of tokens
   1.c The token vault is only accessible to table joins by privileged users
   1.d All other users can only see tokenized data in spark/hive
2. a shell script to pre-create two new kafka topics for for topic-forward (with n partitions) and one for topic-return with 1 partition
3. a shell script to start a Spark 2.x (configurable) spark-submit command
4. A pyspark script to create a spark context (on yarn), connect to the kafka topics, dequeue and save the stream to CSV
   4.a the CSV files are partitioned by day | hour. An hourly compact.py script converts these to day | hour partitioned parquet
   4.b a second spark streaming query writes 20-second summary data to topic-return which is persisted at the source system
   4.c this streaming query can be duplicated for new aggregates which are pulsed back to source using the topic-return topic
5. Various cleanup scripts (delete kafka topics, clear HDFS subdirectory etc)


Process flow:
a. Create a config file BDSxx_{NAME} for example BDS03_GDELT_NEWS with 
