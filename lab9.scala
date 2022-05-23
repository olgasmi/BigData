// Databricks notebook source
val nested_json = spark.read.format("json")
                         .option("inferSchema", "true")
                         .option("multiLine", "true")
                         .load("/FileStore/tables/Nested.json")

// COMMAND ----------

display(nested_json)

// COMMAND ----------

val new_nested_json = nested_json.withColumn("newColumn", $"pathLinkInfo".dropFields("alternateName", "elevationInDirection"))

new_nested_json.withColumn("elevationGain", $"pathLinkInfo".dropFields("alternateName", "captureSpecification", "cycleFacility", "endGradeSeparation", "endNode", 
                                                                       "fictitious", "formOfWay", "formsPartOfPath", "heightingMethod", "matchStatus", "pathName",
                                                                       "startGradeSeparation", "startNode", "surfaceType", "formsPartOfStreet",
                                                                       "sourceFID")).withColumn("elevationAgainstDirection",$"elevationGain"
                                                                                                .dropFields("elevationGain.elevationInDirection"))

display(new_nested_json)

// COMMAND ----------

val list = List(1, 2, 3 ,4)
val result = list.foldLeft(0)(_ + _)
println(result)

// COMMAND ----------

val result = list.foldLeft(20)(_ + _)
println(result)

// COMMAND ----------

val result = list.foldLeft(0)(_ - _)
println(result)

// COMMAND ----------

val result = list.foldLeft(-50)(_ - _)
println(result)

// COMMAND ----------

val result = list.foldLeft(0) { (_, current) => current + 1 }
println(result)

// COMMAND ----------

import org.apache.spark.sql.functions._
val fields = Array("pathName","alternateName")
val excludedNestedFields = Map("pathLinkInfo" -> fields)

// COMMAND ----------

val df = excludedNestedFields.foldLeft(new_nested_json){ (k,v) => (k.withColumn("NewColumn", col(v._1).dropFields(v._2:_*)))}
display(df)
