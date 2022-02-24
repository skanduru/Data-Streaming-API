spark = SparkSession.builder.appName("example") 
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.apache.hadoop:hadoop-aws:3.2.0') 
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) 
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) 
    .master("local[8]") 
    .getOrCreate()

start_df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("startingOffsets", "latest")
    .option("subscribe",KAFKA_TOPIC_INPUT) 
    .load()

... data processing ...

end_df.repartition(3) 
    .writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", KAFKA_SERVER) 
    .option("topic", KAFKA_TOPIC_OUTPUT) 
    .option("checkpointLocation", CHECKPOINT_LOCATION) 
    .trigger(processingTime="1 minutes") 
    .start()

end_df.awaitTermination()
