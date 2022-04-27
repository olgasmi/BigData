# Databricks notebook source
# MAGIC %md
# MAGIC # Zadanie 1
# MAGIC Hive ma ograniczone możliwości w sprawie indeksowania. Nie ma tutaj kluczy w znaczeniu relacyjnych baz danych, ale istnieje możliwość stworzenia indeksu na kolumnach, które są przechowywane w innej tabeli.
# MAGIC Utrzymanie indeksu wymaga dodatkowego miejsca na dysku, a budowanie indeksu jest związane z kosztami przetwarzania, dlatego należy porównań koszty z korzyściami płynącymi z takiego indeksowania.
# MAGIC 
# MAGIC 
# MAGIC Indeksowanie jest alternatywą dla partycjonowania, gdy partycje logiczne byłyby w rzeczywistości zbyt liczne i małe, aby były użyteczne.Jest ono użyteczne w oczyszczeniu niektórych bloków z tabeli jako danych wejściowych dla zadania MapReduce. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Zadanie 3

# COMMAND ----------

catalog = spark.catalog.listDatabases()
display(catalog)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS db")

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/names.csv"
df = spark.read.csv(filePath, header=True)
df.write.mode("overwrite").saveAsTable("db.names")

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/actors.csv"
df = spark.read.csv(filePath, header=True)
df.write.mode("overwrite").saveAsTable("db.actors")

# COMMAND ----------

display(catalog)

# COMMAND ----------

for table in spark.catalog.listTables("db"):
	print(table.name)

# COMMAND ----------

spark.sql("SELECT * FROM db.names").show()

# COMMAND ----------

def function(database):
    for table in spark.catalog.listTables(database):
        spark.sql("DELETE FROM " + database + "." + table.name)

function("db")

# COMMAND ----------

spark.sql("SELECT * FROM db.names").show()
