# Databricks notebook source
#ex1
from pyspark.sql.types import *

schema = StructType([StructField("imdb_name_id",StringType(),True),
                     StructField("name",StringType(),True),
                     StructField("birth_name",StringType(),True),
                     StructField("height",IntegerType(),True),
                     StructField("bio",StringType(),True),
                     StructField("birth_details",StringType(),True),
                     StructField("date_of_birth",StringType(),True),
                     StructField("place_of_birth",StringType(),True),
                     StructField("death_details",StringType(),True),
                     StructField("date_of_death",StringType(),True),
                     StructField("reason_of_death",StringType(),True),
                     StructField("spouses_string",StringType(),True),
                     StructField("spouses",StringType(),True),
                     StructField("divorces",StringType(),True),
                     StructField("spouses_with_children",StringType(),True),
                     StructField("children",StringType(),True)])

filePath = "dbfs:/FileStore/tables/names.csv"
dataFrame = spark.read.option("delimiter", ",").option("header","true").schema(schema).csv(filePath)
#display(dataFrame)

#ex2
schema1 = StructType([StructField("imdb_name_id",StringType(),True),
                     StructField("name",StringType(),True),
                     StructField("birth_name",StringType(),True),
                     StructField("height",IntegerType(),True)])

newDataFrame = dataFrame.select("imdb_name_id", "name", "birth_name", "height")
jsonPath = "/FileStore/tables/ex2.json"
newDataFrame.write.mode('Overwrite').json(jsonPath)

jsonDataFrame = spark.read.format("json").schema(schema1).option("mode", "permissive").load(jsonPath)
#display(jsonDataFrame)

# COMMAND ----------

#ex3
from pyspark.sql.functions import *

columns = ["timestamp","unix", "Date", "current_date", "current_timestamp"]
data = [["2015-03-22T14:13:34", 1646641525847,"May, 2021"], ["2015-03-22T15:03:18", 1646641557555,"Mar, 2021"], ["2015-03-22T14:38:39", 1646641578622,"Jan, 2021"]]

dataFrame = spark.createDataFrame(data).withColumn("current_date",current_date()).withColumn("current_timestamp",current_timestamp()).toDF(columns[0], columns[1], columns[2], columns[3], columns[4])

#dataFrame.printSchema()
display(dataFrame)

#unix_timestamp()
newUnix = dataFrame.select("timestamp",unix_timestamp("timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()
display(newUnix)

#date_format()
dateFormat = dataFrame.select("current_date", date_format("current_date","yyyy-MM-dd"))
display(dateFormat)


#from_unixtime()
fromUnixtime = dataFrame.select("unix", from_unixtime("unix"), from_unixtime("unix", "MM-dd-yyyy HH:mm:ss"), from_unixtime("unix", "MM-dd-yyyy"))
display(fromUnixtime)


#to_date()
toDate = dataFrame.select("current_timestamp",to_date("current_timestamp", "yyyy-MM-dd"))
display(toDate)

#to_timestamp() 
toTimestamp = dataFrame.select("current_date", to_timestamp("current_date", "yyyy-MM-dd"))
display(toTimestamp)

#to_utc_timestamp()
toUtcTimestamp = dataFrame.select("current_date",to_utc_timestamp("current_date", "PST"))
display(toUtcTimestamp)

#from_utc_timestamp()
fromUtcTimestamp = toUtcTimestamp.select("to_utc_timestamp(current_date, PST)",from_utc_timestamp("to_utc_timestamp(current_date, PST)", "PST"))
display(fromUtcTimestamp)

# COMMAND ----------

#ex4
#df = spark.read.option("badRecordsPath", "/tmp/badRecordsPath").parquet("/input/parquetFile")
#dbutils.fs.rm("/input/parquetFile")
#df.show()
#Path does not exist: dbfs:/input/parquetFile -> if df.show() cannot find the input file, then Spark creates an exeception file in JSON forman to record the error 

columns = ["name","surname", "age"]
data = [["Olga", "Smistek", 21], ["Jan", "Kowalski", 25], ["Agnieszka", "Bien", "36"]]

dataFrame = spark.createDataFrame(data).toDF(columns[0], columns[1], columns[2])
display(dataFrame)
schema3 = StructType([StructField("name",StringType(),True),
                     StructField("surname",StringType(),True),
                     StructField("age",IntegerType(),True)])

jsonPath = "/FileStore/tables/ex4.json"
dataFrame.write.mode('Overwrite').json(jsonPath)

jsonDataFrame = spark.read.option("badRecordsPath", "/tmp/badRecordsPath").schema(schema3).json(jsonPath)
display(jsonDataFrame)

#dataFrame will contain only first two rows, the last one is recordded in the exception file, which is a JSON file
#After location the exception files it is possible to use a JSON reader to process them


# COMMAND ----------

columns = ["name","surname", "age"]
data = [["Olga", "Smistek", 21], ["Jan", "Kowalski", 25], ["Agnieszka", "Bien", 26]]

dataFrame = spark.createDataFrame(data).toDF(columns[0], columns[1], columns[2])
display(dataFrame)
schema3 = StructType([StructField("name",StringType(),True),
                     StructField("surname",StringType(),True),
                     StructField("age",IntegerType(),True)])

parquetPath = "/FileStore/tables/ex5parquet"

dataFrame.write.format("parquet").mode("Overwrite").save(parquetPath)
dataFrameParquet = spark.read.format("parquet").load(parquetPath)
display(dataFrameParquet)

#dataFrame.dtypes
dataFrameParquet.dtypes

jsonPath = "/FileStore/tables/ex5.json"
dataFrame.write.mode('Overwrite').json(jsonPath)

jsonDataFrame = spark.read.format("json").schema(schema3).load(jsonPath)
display(jsonDataFrame)
jsonDataFrame.dtypes

#dataFrameParquet, jsonDataFrame is correct comparing to dataFrame; types are the same; when is comes to the order of data, that is displayed, it is reversed comparing to the input dataFrame
