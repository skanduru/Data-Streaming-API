from pyspark.sql import SparkSession

# Kafka is a comm channel. Can also be compared to a mailbox
# create a Spak Session, and name the app something relevant
spark = SparkSession.builder.appName("fuel-level").getOrCreate()

#set the log level to WARN
spark.sparkContext.setLogLevel("WARN")


# read a stream from the kafka topic 'balance-updates', with the bootstrap server kafka:19092, reading from the earliest message
kafkaRawStreamingDF = spark \
  .readStream                            \
  .format("kafka")                       \
  .option("kafka.bootstrap.server", "localhost:9092")  \
  .option("subscribe", "fuel-level")  \
  .option("startingOffsets", "earliest")  \
  .load()

# cast the key and value columns as strings and select them using a select expression function
kafkaStreamingDF = kafkaRawStreamDF.selectexpr("cast(key as string) key",
       "cast(value as string) value")

#TO-DO: write the dataframe to the console, and keep running indefinitely
kafkaStreamingDf.writeStream.outputMode("append").format("console").start().awaitTermination()
