#!/bin/ksh
#
# This script auto-generates scripts and configuration files to start a new continuous processing stream
# using SAP Cloud Platform Big Data Services (formerly Altiscale)
# SAP HANA (as a repository database)
# SAP HANA Streaming Analytics (formerly Sybase ESP) to process incoming data on-prem, tokenize sensitive data and send to kafka
# Apache Kafka to send data from on-prem to Spark (on cloud) with configurable topics, partitions and retention
# Spark Streaming to recieve streaming data from kafka, persist it to HDFS and returned pulsed aggregates to kafka
# Grafana to monitor each stream for GB/hr throughput (forwards) and messages/hour (return) 
# Prometheus to store time-series data for Grafana
# Solr to build a text index of streamed data on HDFS.
# (note that automated reconfiguration of grafana, prometheus and Solr has not yet been included in this script release).
#
#
# Usage: sh 01_BuildStream.sh parameterfile
# for example sh 01_BuildStream.sh BDS03_GDELT_EVENTS
# The parameter file must be in the same directory.
#
#
PARFILE=${1}
if [ ${PARFILE}x = x ]
then
   echo "(E) usage: 01_buildStream.sh parameterfile"
   exit 255
fi
if [ ! -f ${PARFILE} ]
then
   echo "(E) parameter file ${PARFILE} not found in `pwd`"
   exit 255
fi
. ./${PARFILE}



TS=`date +"%Y%m%d%H%M%S"`
D=/home/pentos/continuous-streams
ROOT_STREAM_DIR=/hana/data_streaming/PPN/ppn/adapters/default/tmp
SCRIPTS="/home/pentos/scripts"

ROOT_HDFS=/user/markteehan
ROOT_BDS=/home/markteehan/02_poc

TOPIC=${STREAMNAME}
if [ "${TOPIC_IS_UNIQUE}" = "YES" ]
then
    KAFKA_TOPIC=${TOPIC}_${TS}
else
    KAFKA_TOPIC=${TOPIC}
fi

if [ "${INPUT_DELIMITER}"x = x ]
then
   INPUT_DELIMITER=","
fi
if [ "${OUTPUT_DELIMITER}"x = x ]
then
   OUTPUT_DELIMITER=","
fi


HDFS_DIR=${ROOT_HDFS}/${TOPIC}

mkdir ${D}/${TS}_${TOPIC}
cd ${D}/${TS}_${TOPIC}

ZOOKEEPER="XXX.service.XXX.com:ppppp"
    KAFKA="XXX.service.XXX.com:ppppp"
      BDS="XXXXXXXXXX"

TMP=/tmp/create_${TOPIC}

# generate string for column names from the table definition. 
vCreate="`python ${SCRIPTS}/02_buildStream.py ${TOPIC} 1`"
vSelect="`python ${SCRIPTS}/02_buildStream.py ${TOPIC} 2`"
vSelectExpr="`python ${SCRIPTS}/02_buildStream.py ${TOPIC} 3`"
vSolrSchema="`python ${SCRIPTS}/02_buildStream.py ${TOPIC} 4`"


if [ "${HDFS_DIR_OVERRIDE}"x = x ]
then
  HDFS_CCK_DIR=${ROOT_HDFS}/${KAFKA_TOPIC}_cck
  HDFS_RETURN_CCK_DIR=${ROOT_HDFS}/${KAFKA_TOPIC}_return_cck
  RMCMD1="hdfs dfs -rm -r ${HDFS_DIR}"
  RMCMD2="hdfs dfs -rm -r ${HDFS_CCK_DIR}"
  RMCMD2="hdfs dfs -rm -r ${HDFS_RETURN_CCK_DIR}"
else
  HDFS_DIR=${ROOT_HDFS}/${HDFS_DIR_OVERRIDE}
  HDFS_CCK_DIR=${ROOT_HDFS}/${HDFS_CCK_DIR_OVERRIDE}
  HDFS_RETURN_CCK_DIR=${ROOT_HDFS}/${HDFS_RETURN_CCK_DIR_OVERRIDE}
  RMCMD1="# No hdfs delete: using override directory ${HDFS_DIR} "
  RMCMD2=" "
  RMCMD2=" "
fi

cat <<EOF >${TOPIC}_solr_schema.xml
<?xml version="1.0" encoding="UTF-8" ?> <schema name="minimalconfig" version="1.6"> <uniqueKey>id</uniqueKey>
${vSolrSchema}
<fieldType name="string"   class="solr.StrField"       sortMissingLast="true" />
<fieldType name="VARCHAR"  class="solr.StrField"       sortMissingLast="true" />
<fieldType name="NVARCHAR" class="solr.StrField"       sortMissingLast="true" />
<fieldType name="DECIMAL"  class="solr.TrieFloatField" sortMissingLast="true" />
<fieldType name="INTEGER"  class="solr.TrieIntField"   sortMissingLast="true" />
<fieldType name="text_basic" class="solr.TextField" positionIncrementGap="100">
<analyzer>
  <tokenizer class="solr.StandardTokenizerFactory"/>
  <filter class="solr.LowerCaseFilterFactory"/>
</analyzer>
</fieldType>
</schema>
EOF




cat <<EOF >${TS}_hive.sh
# not yet implemented
drop table if exists default.${STREAMNAME}
CREATE external TABLE default.${STREAMNAME} (
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('field.delim'=',','serialization.format'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION "hdfs://${HDFS_DIR}";
EOF


cat <<EOF > ${TOPIC}_bds_cmds.sh
hdfs dfs -mkdir ${TOPIC}
EOF

# drop directory
cat <<EOF > ${TOPIC}_onprem_cmds.sh
mkdir ${ROOT_STREAM_DIR}/${STREAMNAME}
EOF


# CCL - on-prem script to stream data. Excludes code for Vaulting and tokenization.
cat <<EOF >${TOPIC}.CCL
// ${TOPIC}.CCL section 1 - HANA SSA -> BDS
${vCreate}

CREATE INPUT stream stream_${STREAMNAME} SCHEMA ${STREAMNAME}_schema;

CREATE LOCAL STREAM stream_${STREAMNAME}_local SCHEMA ${STREAMNAME}_schema AS
${vSelect}


ATTACH  INPUT ADAPTER ${STREAMNAME}_csv TYPE toolkit_file_csv_input TO stream_${STREAMNAME}  PROPERTIES
 dir = '/tmp/${STREAMNAME}',file = '.*\.${INPUT_EXTENSION}',dynamicMode = 'dynamicPath'
,csvMatchStreamName = FALSE,csvQuotedValues = '',removeAfterProcess = TRUE,csvDelimiter ='${INPUT_DELIMITER}',csvSecondDateFormat = 'yyyyMMddHHmmss',pollingPeriod = 2 ;

CREATE OUTPUT STREAM stream_${STREAMNAME}_output as select * from stream_${STREAMNAME}_local where 1=1;
ATTACH  OUTPUT ADAPTER Kafka_${STREAMNAME} TYPE toolkit_kafka_csv_output TO stream_${STREAMNAME}_output PROPERTIES kafkaTopicPartition = '${KAFKA_TOPIC}' ,
      kafkaBootstrapServers = '${KAFKA}' ,
      kafkaProducerProperties = './producer.properties',outputBase = TRUE,csvPrependStreamNameOpcode = FALSE,gdBatchSize = 1 ,
      gdBatchPeriod = 0,onlyBase = FALSE,enableGdMode = FALSE ,enableGdCache = FALSE ,gdSubscriberName = '' ,
      csvDelimiter = '' ,csvSecondDateFormat = 'yyyyMMddHHmmss' ,csvMsDateFormat = '' ,
      csvTimeFormat = '' ,csvBigdatetimeFormat = '' ,csvHasHeader = FALSE ;

// section 2 - BDS -> HANA SSA
CREATE INPUT STREAM ${STREAMNAME}_return SCHEMA (col_key string,col_value string);

ATTACH  Input ADAPTER ${STREAMNAME}_kafka_adapter TYPE toolkit_kafka_csv_input TO ${STREAMNAME}_return PROPERTIES kafkaTopicPartition = '${KAFKA_TOPIC}_RETURN'
,kafkaBootstrapServers = '${KAFKA}',kafkagroupId='${KAFKA_TOPIC}_RETURN';

CREATE OUTPUT STREAM ${STREAMNAME}_hana as select * from ${STREAMNAME}_return where 1=1;
ATTACH OUTPUT ADAPTER ${STREAMNAME}_HANA_adapter TYPE hana_out TO ${STREAMNAME}_hana PROPERTIES service = '${HOSTNAME}_ppn',
sourceSchema = 'POC_02' , table = 'BDS_RETURN',
outputBase = FALSE,useUpserts = FALSE,permutation='col_key=KEY:col_value=VALUE',maxWorkers = 6,
maxCommitInterval = 1000 ,maxCommitSize = 25000,dataWarehouseMode = 'OFF',timestampColumnName = '',opcodeColumnName = '',onlyBase = FALSE ,
timezoneForStats = 'GMT',reconnectAttemptDelayMSec = 1000,maxReconnectAttempts = 1,enableGDMode = FALSE,enableGDCache = TRUE,gdBatchSize = 10 ,
bulkBatchSize = 10000,bulkInsertArraySize = 1000,idleBufferWriteDelayMSec = 1000,bufferAgeLimitMSec = 10000,threadCount = 1 ,maxQueueSize = 0;

EOF


# Kafka
cat <<EOF > ${KAFKA_TOPIC}_kafka.sh
  export KAFKA_OPTS=
  /home/markteehan/02_poc/02_kafka/kafka/bin/kafka-topics.sh  --create --zookeeper ${ZOOKEEPER} --topic ${KAFKA_TOPIC} --partitions ${STREAMS} --replication-factor 1
  /home/markteehan/02_poc/02_kafka/kafka/bin/kafka-configs.sh --alter  --zookeeper ${ZOOKEEPER} --entity-name ${KAFKA_TOPIC} --entity-type topics --add-config retention.bytes=${KAFKA_RETENTION_BYTES}

  /home/markteehan/02_poc/02_kafka/kafka/bin/kafka-topics.sh  --create --zookeeper ${ZOOKEEPER} --topic       ${KAFKA_TOPIC}_RETURN --partitions 1 --replication-factor 1
  /home/markteehan/02_poc/02_kafka/kafka/bin/kafka-configs.sh --alter  --zookeeper ${ZOOKEEPER} --entity-name ${KAFKA_TOPIC}_RETURN --entity-type topics --add-config retention.bytes=${KAFKA_RETENTION_BYTES}
EOF

cat <<EOF > ${KAFKA_TOPIC}_kafka_delete.sh
 export KAFKA_OPTS=
 /home/markteehan/02_poc/02_kafka/kafka/bin/kafka-topics.sh  --delete --zookeeper ${ZOOKEEPER} --topic ${KAFKA_TOPIC}
 /home/markteehan/02_poc/02_kafka/kafka/bin/kafka-topics.sh  --delete --zookeeper ${ZOOKEEPER} --topic ${KAFKA_TOPIC}_RETURN

# # Zookeeper bug - "marked for deletion" never disappears. Delete it manually from ZK
#/home/markteehan/02_poc/02_kafka/kafka/bin/zookeeper-shell.sh  ${ZOOKEEPER} <<EEE
#  rmr /brokers/topics/${KAFKA_TOPIC}
#  rmr /brokers/topics/${KAFKA_TOPIC}_RETURN
#EE

EOF


#Spark
read -d '' vString  << EOF
#${TOPIC}.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
vWorkers=${STREAMS}
vConcurrentJobs=3

spark = SparkSession \
      .builder.appName("${STREAMNAME}")                    \
      .enableHiveSupport()                                 \
      .config("spark.executor.instances", vWorkers)        \
      .config("spark.yarn.am.cores","5")                   \
      .config("spark.yarn.am.memory","3g")                 \
      .config("spark.executor.cores","5")                  \
      .config("spark.executor.memory","5g")                \
      .config("spark.yarn.executor.memoryOverhead","1024") \
      .config("hive.exec.dynamic.partition", "true")       \
      .config("hive.exec.dynamic.partition.mode", "nonstrict")   \
      .config("spark.streaming.backpressure.enabled", "true")    \
      .config("spark.streaming.concurrentJobs", vConcurrentJobs) \
      .config("spark.metrics.conf.*.sink.graphite.class","org.apache.spark.metrics.sink.GraphiteSink")   \
      .config("spark.metrics.conf.*.sink.graphite.host","XX.XX.XX.XX") \
      .config("spark.metrics.conf.*.sink.graphite.port","ppppp") \
      .config("spark.metrics.conf.*.sink.graphite.period","5") \
      .config("spark.metrics.conf*.sink.graphite.unit","seconds") \
      .config("spark.metrics.conf.master.source.jvm.class","org.apache.spark.metrics.source.JvmSource")\
      .config("spark.metrics.conf.worker.source.jvm.class","org.apache.spark.metrics.source.JvmSource")\
      .config("spark.metrics.conf.driver.source.jvm.class","org.apache.spark.metrics.source.JvmSource")\
      .config("spark.metrics.conf.executor.source.jvm.class","org.apache.spark.metrics.source.JvmSource") \
      .getOrCreate()

${STREAMNAME}_df = spark \
      .readStream \
      .format("kafka") \
      .option("zookeeper.connect", "${ZOOKEEPER}") \
      .option("kafka.bootstrap.servers", "${KAFKA}") \
      .option("subscribe","${KAFKA_TOPIC}")      \
      .option("startingOffsets", "earliest")    \
      .option("failOnDataLoss", "false")        \
      .load()

# unpack the kafka value, add the timestamp
${STREAMNAME}=${STREAMNAME}_df.select (col("value").cast("string"),col("timestamp").cast("string") ).alias("c").select("c.value","c.timestamp")

${STREAMNAME}_2=${STREAMNAME}.${vSelectExpr}

${STREAMNAME}_4=${STREAMNAME}_2                                           \
      .writeStream                                                        \
      .queryName("Q_${STREAMNAME}_01")                                    \
      .format("csv")                                                      \
      .option("delimiter","${OUTPUT_DELIMITER}")                          \
      .partitionBy("partition_key")                                       \
      .option("path","${HDFS_DIR}")                    \
      .option("checkpointLocation", "${HDFS_CCK_DIR}") \
      .start()


# value is (StreamName, MetricID, TS, NumericData, StringData)
${STREAMNAME}_RETURN_A=${STREAMNAME}_2   \
 .withWatermark("sps_ts", "20 seconds")  \
 .groupBy("sps_ts","partition_key")      \
 .count()                                \
 .selectExpr("                      \
            '${STREAMNAME}' AS key" \
            ,"concat(               \
             '${STREAMNAME}'        \
        ,'|'                    \
        ,'C01'                  \
        ,'|'                    \
        ,CAST(sps_ts as STRING) \
        ,'|'                    \
        ,CAST(count as STRING)  \
        ,'|'                    \
        ,CAST('' as STRING)     \
        ) as value")

${STREAMNAME}_RETURN_B=${STREAMNAME}_RETURN_A \
     .writeStream                                                \
     .queryName("Q_${STREAMNAME}_RETURN_B")                      \
     .format("kafka")                                            \
     .option("kafka.bootstrap.servers", "${KAFKA}")   \
     .option("topic","${KAFKA_TOPIC}_RETURN")                     \
     .trigger(processingTime="20 seconds")                       \
     .option("checkpointLocation", "${HDFS_RETURN_CCK_DIR}") \
     .start()

${STREAMNAME}_4.awaitTermination()
EOF
echo "$vString" > ${TOPIC}.py
set +x

#METER_INTERVAL.sh
cat <<EOF > ${TOPIC}_sparkstreaming.sh
#!/bin/sh

TMP=/home/markteehan/02_poc/tmp
DATA=/home/markteehan/02_poc/data
ENV=/home/markteehan/02_poc/env
LOG=/home/markteehan/02_poc/log
SRC=/home/markteehan/02_poc/source
BIN=/home/markteehan/02_poc/bin
CONFIG=/home/markteehan/02_poc/config
. \${ENV}/setenv_spark211
export PATH=\$SPARK_HOME/bin:\${SPARK_HOME}/lib:\${PATH}

TS=\`date +"%Y%m%d%H%M%S"\`
if [ -f "\${LOG}/${TOPIC}_sparkstreaming.log" ]
then
  mv \${LOG}/${TOPIC}_sparkstreaming.log \${LOG}/${TOPIC}_\${TS}_sparkstreaming.log
fi
echo;echo;echo;echo "Starting a continuous bds_stream for ${KAFKA_TOPIC} using Spark on Yarn and pyspark script ${TOPIC}.py into logfile ${TOPIC}_sparkstreaming.log"
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1  --master "yarn" --deploy-mode "client" ${TOPIC}.py >>${TOPIC}_sparkstreaming.log 2>&1 &
echo run this:
echo tail -f nohup.out ${TOPIC}_sparkstreaming.log
EOF

# Monitor HDFS
cat <<EOF > ${TOPIC}_monitor_hdfs.sh
#!/bin/sh
hdfs dfs -du -h  ${TOPIC}
EOF

# cleanup
cat <<EOF > ${KAFKA_TOPIC}_delete
#!/bin/sh
echo;echo;echo "About to shutdown and delete all scripts and data for stream ${KAFKA_TOPIC}"
echo;echo;echo "Clear up the kafka cache first by running /home/markteehan/02_poc/02_kafka/kafka/${KAFKA_TOPIC}_kafka_delete.sh"
echo;echo "Enter Y to continue"
read hello
if [ "\${hello}" = "Y" ]
then
  echo "File cleanup will run in five seconds"
  sleep 5
  ${RMCMD1}
  ${RMCMD2}
  ${RMCMD3}
  rm -f ../source/${TOPIC}.py
  rm -f ../scripts/${TOPIC}_sparkstreaming.sh
  rm -f ../scripts/${KAFKA_TOPIC}_kafka.sh
  rm -f ../scripts/${KAFKA_TOPIC}_delete.sh
  rm -f ../scripts/${TOPIC}_monitor_hdfs.sh
  rm -f ../scripts/${TOPIC}_delete.sh

fi
EOF


set -x
echo;echo;echo "Copy commands:"

chmod +x ${TOPIC}_sparkstreaming.sh ${TOPIC}_monitor_hdfs.sh ${KAFKA_TOPIC}_kafka.sh ${KAFKA_TOPIC}_kafka_delete.sh
ssh ${BDS} mkdir -p "${ROOT_BDS}/bds_streams/${TOPIC}"
scp ${TOPIC}.py                ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${TOPIC}.py
scp ${TOPIC}_sparkstreaming.sh ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${TOPIC}_sparkstreaming.sh
scp ${TOPIC}_monitor_hdfs.sh   ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${TOPIC}_monitor_hdfs.sh
scp ${TOPIC}_delete            ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${TOPIC}_delete.sh
scp ${TOPIC}_solr_schema.xml   ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${TOPIC}_solr_schema.xml

scp ${KAFKA_TOPIC}_kafka.sh          ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${KAFKA_TOPIC}_kafka.sh
scp ${KAFKA_TOPIC}_kafka_delete.sh   ${BDS}:${ROOT_BDS}/bds_streams/${TOPIC}/${KAFKA_TOPIC}_kafka_delete.sh

