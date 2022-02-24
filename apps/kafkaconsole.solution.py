from pyspark.sql import SparkSession
# import rpdb

# the source for this data pipeline is a kafka topic, defined below
# rpdb.set_trace()
spark = SparkSession.builder.appName("balance-events").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","balance-updates")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#this is necessary for Kafka Data Frame to be readable, into a single column  value
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this takes the stream and "sinks" it to the console as it is updated one at a time like this:
# +--------------------+-----+
# |                 Key|Value|
# +--------------------+-----+
# |1593939359          |13...|
# +--------------------+-----+
# kafkaStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
kafkaStreamingDF.selectExpr("cast(key as string) as key", "to_json(struct(*)) as value")\
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "kafkacons-status") \
  .option("checkpointLocation", "/tmp/kafkacheckpoint4") \
  .start() \
  .awaitTermination()
