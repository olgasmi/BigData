// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1 
// MAGIC 
// MAGIC Wykorzystaj przykłady z notatnika Windowed Aggregate Functions i przepisz funkcje używając Spark API.

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Sample")

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val transactionSchema = StructType(Array(StructField("AccountID", IntegerType, true), StructField("TransDate", StringType, true), StructField("TranAmt", DoubleType, true)))
val logicalSchema = StructType(Array(StructField("RowID", IntegerType, true), StructField("FName", StringType, true), StructField("Salary", IntegerType, true)))

// COMMAND ----------

import org.apache.spark.sql.Row

val data = Seq(Row( 1, "2011-01-01", 500.0),
Row( 1, "2011-01-15", 50.0),
Row( 1, "2011-01-22", 250.0),
Row( 1, "2011-01-24", 75.0),
Row( 1, "2011-01-26", 125.0),
Row( 1, "2011-01-28", 175.0),
Row( 2, "2011-01-01", 500.0),
Row( 2, "2011-01-15", 50.0),
Row( 2, "2011-01-22", 25.0),
Row( 2, "2011-01-23", 125.0),
Row( 2, "2011-01-26", 200.0),
Row( 2, "2011-01-29", 250.0),
Row( 3, "2011-01-01", 500.0),
Row( 3, "2011-01-15", 50.0),
Row( 3, "2011-01-22", 5000.0),
Row( 3, "2011-01-25", 550.0),
Row( 3, "2011-01-27", 95.0),
Row( 3, "2011-01-30", 2500.0))

val transactionDf = spark.createDataFrame(spark.sparkContext.parallelize(data), transactionSchema)
display(transactionDf)

// COMMAND ----------

val data = Seq(Row(1,"George", 800),
        Row(2,"Sam", 950),
        Row(3,"Diane", 1100),
        Row(4,"Nicholas", 1250),
        Row(5,"Samuel", 1250),
        Row(6,"Patricia", 1300),
        Row(7,"Brian", 1500),
        Row(8,"Thomas", 1600),
        Row(9,"Fran", 2450),
        Row(10,"Debbie", 2850),
        Row(11,"Mark", 2975),
        Row(12,"James", 3000),
        Row(13,"Cynthia", 3000),
        Row(14,"Christopher", 5000))

val logicalDf = spark.createDataFrame(spark.sparkContext.parallelize(data), logicalSchema)
display(logicalDf)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val window  = Window.partitionBy($"AccountID").orderBy($"TransDate")

transactionDf.withColumn("RunTotalAmt",sum($"TranAmt") over window ).orderBy($"AccountID",$"TransDate").show()

// COMMAND ----------

val window = Window.partitionBy($"AccountID").orderBy($"TransDate")

transactionDf.withColumn("RunAvg", avg($"TranAmt") over window)
             .withColumn("RunTranQty", count("*") over window)
             .withColumn("RunSmallAmt", min($"TranAmt") over window)
             .withColumn("RunLargeAmt", max($"TranAmt") over window)
             .withColumn("RunTotalAmt", sum($"TranAmt") over window).orderBy($"AccountID",$"TransDate").show()

// COMMAND ----------

val window  = Window.partitionBy($"AccountID").orderBy($"TransDate").rowsBetween(-2, 0)
 
transactionDf.withColumn("SlideAvg", avg($"TranAmt") over window)
             .withColumn("SlideQty", count("*") over window)
             .withColumn("SlideMin", min($"TranAmt") over window)
             .withColumn("SlideMax", max($"TranAmt") over window)
             .withColumn("SlideTotal", sum($"TranAmt") over window)
             //.withColumn("RN", row_number().over(window))
             .orderBy($"AccountID",$"TransDate").show()

// COMMAND ----------

val window = Window.orderBy($"Salary").rowsBetween(Window.unboundedPreceding, 0)

logicalDf.withColumn("SumByRows", sum($"Salary") over window).orderBy($"RowID").show()

// COMMAND ----------

val window = Window.orderBy($"Salary").rangeBetween(Window.unboundedPreceding, 0)

logicalDf.withColumn("SumByRange", sum($"Salary") over window).orderBy($"RowID").show()

// COMMAND ----------

val window =  Window.partitionBy($"AccountID").orderBy($"TransDate")
transactionDf.withColumn("RN", row_number().over(window)).orderBy($"TransDate").limit(10).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2
// MAGIC 
// MAGIC Użyj ostatnich danych i użyj funkcji okienkowych LEAD, LAG, FIRST_VALUE, LAST_VALUE, ROW_NUMBER i DENS_RANK - każdą z funkcji wykonaj dla ROWS i RANGE i BETWEEN. 

// COMMAND ----------

val window  = Window.partitionBy($"AccountID").orderBy($"TransDate")

transactionDf.withColumn("RunLead",lead($"TranAmt",1) over window)
              .withColumn("RunLag",lag($"TranAmt",1) over window)
              .withColumn("RunFirstValue",first($"TranAmt") over window.rowsBetween(-3, 0))
              .withColumn("RunLastValue",last($"TranAmt") over window.rowsBetween(-3, 0))
              .withColumn("RunRowNumber",row_number() over window )
              .withColumn("RunDensRank",dense_rank() over window)
              .orderBy($"AccountID",$"TransDate").show()

// COMMAND ----------

transactionDf.withColumn("RunFirstValue",first($"TranAmt") over window.rangeBetween(Window.unboundedPreceding, 0))
             .withColumn("RunLastValue",last($"TranAmt") over window.rangeBetween(Window.unboundedPreceding, 0)).orderBy($"AccountID",$"TransDate").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3
// MAGIC 
// MAGIC Użyj ostatnich danych i wykonaj połączenia Left Semi Join, Left Anti Join, za każdym razem sprawdź .explain i zobacz jak spark wykonuje połączenia. Jeśli nie będzie danych to trzeba je zmodyfikować żeby zobaczyć efekty. 

// COMMAND ----------

val table = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderdetail")

display(table)

// COMMAND ----------

val table2 = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")

display(table2)

// COMMAND ----------

val joinLeft = table.join(table2, table2.col("SalesOrderID") === table.col("SalesOrderID"), "leftsemi")
joinLeft.show()

joinLeft.explain()

// COMMAND ----------

val joinLeftAnti = table.join(table2, table.col("SalesOrderID") === table2.col("SalesOrderID"), "leftanti")
joinLeftAnti.show()

joinLeftAnti.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4
// MAGIC 
// MAGIC Połącz tabele po tych samych kolumnach i użyj dwóch metod na usunięcie duplikatów.  

// COMMAND ----------

display(joinLeft.distinct())

// COMMAND ----------

display(joinLeft.dropDuplicates())

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 5  
// MAGIC 
// MAGIC W jednym z połączeń wykonaj broadcast join i sprawdź plan wykonania. 

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val joinBroadcast = table2.join(broadcast(table), table2.col("SalesOrderID") === table.col("SalesOrderID"))
joinBroadcast.explain()//.show()
