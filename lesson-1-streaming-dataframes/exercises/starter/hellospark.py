from pyspark.sql import SparkSession
import pdb

# TO-DO: create a variable with the absolute path to the text file
logFile = "/home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
logData = spark.read.text(logFile).cache()

# TO-DO: create a global variable for number of times the letter a is found
numAs = logData.filter(logData.value.contains("a")).count()

# TO-DO: create a global variable for number of times the letter b is found
numBs = logData.filter(logData.value.contains("b")).count()

# TO-DO: create a function which accepts a row from a dataframe, which has a column called value
# in the function increment the a count variable for each occurrence of the letter a
# in the value column
print("*************************")
print(f"*****Lines with 'a' and 'b' are {numAs} and {numBs}******")
print("*************************")
# TO-DO: create another function which accepts a row from a dataframe, which has a column called value
# in the function increment the b count variable for each occurrence of the letter b
# in the value column



# TO-DO: use the forEach method to invoke the a counting method
# TO-DO: use the forEach method to invoke the b counting method

# TO-DO: stop the spark application
spark.stop()
