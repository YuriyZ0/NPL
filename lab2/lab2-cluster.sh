export  HADOOP_CONF_DIR=$HADOOP_HOME/conf
PYSPARK_PYTHON=python3 spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2\
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=5 \
  --conf spark.sql.shuffle.partitions=5 \
  --conf spark.default.parallelism=5 \
  main.py



#  --master yarn \
#  --deploy-mode cluster \

#  --conf spark.executor.instances=3 \

#  --conf spark.shuffle.service.enabled=true \
#  --conf spark.dynamicAllocation.enabled=true \

