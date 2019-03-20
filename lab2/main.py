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

batch_from_kafka.select(batch_from_kafka['value'].cast("String")).show(40, False)

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
  .option("startingOffsets", "earliest") \
  .load()
​
​

from_kafka_df = stream_from_kafka \
  .select(stream_from_kafka['value'].cast("String")) \
  .select(from_json(col('value'), schema).alias('parsed')) \
  .drop('_corrupt_record') \
  .select(to_timestamp(col('parsed.timestamp')).alias('ts'), col('parsed.*')) \
  .na.drop('all') 

agg_df = from_kafka_df.groupBy(window("ts", "5 minutes")) \
  .agg(sum('item_price').alias('sales_val'),
      count('timestamp').alias('sales_vol'), 
      count('*').alias('count_all')) \
  .select(to_json(struct(col('*'))).alias('value'))

out = agg_df.writeStream \
  .format("kafka") \
  .outputMode("complete") \
  .trigger(processingTime = "10 seconds") \
  .option("kafka.bootstrap.servers", "10.132.0.5:6667") \
  .option("topic", "out.yury.zenin") \
  .option("checkpointLocation", "out.yury.zenin.task5") \
  .start()

#обязательно для честного запуска:

out.awaitTermination()
