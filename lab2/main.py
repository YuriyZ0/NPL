import sys, os
sys.path.insert(0, '/usr/hdp/current/spark2-client/python/lib/pyspark.zip')
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
​

os.environ['SPARK_HOME'] = '/usr/hdp/current/spark2-client'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 pyspark-shell'

spark = SparkSession.builder \
    .appName("SimpleStreamingApp") \
    .config("spark.jars", "spark-sql-kafka-0-10_2.11-2.3.2.jar") \
    .config("spark.sql.shuffle.partitions", 10) \
    .config("spark.default.parallelism", 10) \
    .getOrCreate()
​
​
    #.config("spark.streaming.batch.duration", 10) \
    # для кафки не работает - .config("spark.sql.streaming.schemaInference", True) 

batch_from_kafka = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.132.0.5:6667") \
  .option("subscribe", "yury.zenin") \
  .option("startingOffsets", "earliest") \
  .option("endingOffsets", "latest") \
  .load()

batch_from_kafka.select(from_json(batch_from_kafka['value'].cast("String"), schema).alias('parsed')) \
    .select(to_timestamp(col('parsed.timestamp')).alias('ts')) \
    .groupBy(window("ts", "1 hours")) \
    .count() \
    .sort('window')\
    .show(40, False)

batch_from_kafka.select( col('timestamp')) \
    .groupBy(window("timestamp", "1 hours")) \
    .count() \
    .sort('window')\
    .show(40, False)


schema = StructType([StructField("item_url", StringType(), True),
    StructField("basket_price", StringType(), True),
    StructField("item_price", FloatType(), True),
    StructField("detectedDuplicate", BooleanType(), True),
    StructField("timestamp", FloatType(), True),
    StructField("remoteHost", StringType(), True),
    StructField("pageViewId", StringType(), True),
    StructField("sessionId", StringType(), True),
    StructField("detectedCorruption", BooleanType(), True),
    StructField("partyId", StringType(), True),
    StructField("location", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("referer", StringType(), True),
    StructField("userAgentName", StringType(), True),    
    StructField("firstInSession", BooleanType(), True),])

stream_from_kafka = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.132.0.5:6667") \
  .option("subscribe", "yury.zenin") \
  .option("startingOffsets", "latest") \
  .option('maxOffsetsPerTrigger', 30000) \
  .load()
​
​

from_kafka_df = stream_from_kafka \
  .select(from_json(stream_from_kafka['value'].cast("String"), schema).alias('parsed'), 
         stream_from_kafka['timestamp'].alias('kafka_ts')) \
  .drop('_corrupt_record') \
  .na.drop('all') 

agg_df = from_kafka_df \
  .select(to_timestamp(col('parsed.timestamp')).alias('ts'),
          col('kafka_ts'), 
          current_timestamp().alias('spark_executor_ts'), 
          col('parsed.*')) \
  .groupBy(window("ts", "5 minutes")) \
  .agg(sum('item_price').alias('sales_val'),
      count('timestamp').alias('sales_vol'), 
      count('*').alias('count_all'),
      max('spark_executor_ts').alias('max_spark_executor_ts'),
      min('kafka_ts').alias('min_kafka_ts'),
      max(unix_timestamp('spark_executor_ts')-unix_timestamp('kafka_ts')).alias('max_lag')) \
  .select(to_json(struct(col('*'))).alias('value'))

out = agg_df.writeStream \
  .format("kafka") \
  .outputMode("update") \
  .trigger(processingTime = "15 seconds") \
  .option("kafka.bootstrap.servers", "10.132.0.5:6667") \
  .option("topic", "out.yury.zenin") \
  .option("checkpointLocation", "out.yury.zenin.task9") \
  .start()
#обязательно для честного запуска:

out.awaitTermination()

