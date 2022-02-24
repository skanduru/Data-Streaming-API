from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
DepositSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", StringType()),
        StructField("dateAndTime", StringType())
    ]
)

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("Bank-deposits").getOrCreate()
        

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible 
bankDepositRawStreamingDF = spark \
        .readStream \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "bank-deposits")\
        .option("startingOffsets", "earliest")\
        .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bankDepositStreamingDF = bankDepositRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")
        

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 


# TO-DO: create a temporary streaming view called "BankDeposits" 
# it can later be queried with spark.sql
bankDepositStreamingDF.withColumn("value",from_json("value", DepositSchema))\
        .select(col("value.*"))\
        .createOrReplaceTempView("BankDeposits")
        
#TO-DO: using spark.sql, select * from BankDeposits
bankDepositsSelectStartDF = spark.sql("select * from BankDeposits")

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-------------+------+--------------------+
# |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+
# |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+


bankDepositsSelectStartDF.writeStream.outputMode("append").format("console").start().awaitTermination()
