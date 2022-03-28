// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(table)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import io.delta.tables._

val SalesLT = table.where("TABLE_SCHEMA == 'SalesLT'")

val SalesLT_tables_names = SalesLT.select("TABLE_NAME").as[String].collect.toList

for( i <- SalesLT_tables_names)
{
  val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()

  table.write.format("delta").mode("overwrite").saveAsTable(i)
}


// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when,count}


def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{ count(when(col(c).isNull,c)).alias(c) })
}

for( i <- SalesLT_tables_names)
{
  val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  print("\nCounted NULLS for each column in table: " + i +"\n")
  table.select(countCols(table.columns):_*).show()
}



// COMMAND ----------

//all NULLs will be replaced by 0
for( i <- SalesLT_tables_names)
{
  val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()

  val replacedNulls = table.na.fill("0")
  display(replacedNulls)
}


// COMMAND ----------

//dropping Nulls 

for( i <- SalesLT_tables_names)
{
  val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()

  table.na.drop("any")
}

// COMMAND ----------

import org.apache.spark.sql.functions._

val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.SalesOrderHeader")
  .load()

//TaxAmt
println("TaxAmt:")
table.select(last("TaxAmt").alias("last"), min("TaxAmt").alias("min"), max("TaxAmt").alias("max")).show()

//Freight
println("Freight:")
table.select(first("Freight").alias("first"), avg("Freight").alias("avg"), sum("Freight").alias("sum")).show()


// COMMAND ----------

val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.Product")
  .load()

//groupBy - collects identical data into groups; then perform aggregate functions on these groups
table.groupBy("ProductModelId").count()//.show()
table.groupBy("Color").sum()//.show()
table.groupBy("ProductCategoryID").min("Weight")//.show()

//agg + Map
table.groupBy("Size").agg(Map("Weight" -> "avg"))//.show()
table.groupBy("Color").agg(Map("StandardCost" -> "min"))
table.groupBy("SellStartDate").agg(Map("SellEndDate" -> "max"))
table.groupBy("Size").agg(Map("StandardCost" -> "avg"))
table.groupBy("ProductCategoryID").agg(Map("ProductModelID" -> "count")).show()



// COMMAND ----------

import org.apache.spark.sql.functions.udf
import scala.math.sqrt

val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.Product")
  .load()


val incrementProductID = udf((n: Int) => n + 1)
val rootOfListPrice = udf((n: Double) => sqrt(n))
val changedString = udf((n: String) => "_" + n + "_" )


val newTable = table.withColumn("incrementedProductID", incrementProductID(col("ProductID"))).withColumn("rootOfListPrice", rootOfListPrice(col("ListPrice"))).withColumn("changedString", changedString(col("Color")))
display(newTable)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when,count}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};


val df_json = spark.read.option("multiline", "true").json("/FileStore/tables/brzydki.json")

df_json.printSchema()
display(df_json)


