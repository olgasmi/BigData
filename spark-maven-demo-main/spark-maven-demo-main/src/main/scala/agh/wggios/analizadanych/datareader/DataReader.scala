package agh.wggios.analizadanych.datareader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataReader {

  def read_csv(path: String, sql_context: SQLContext, schema: StructType = null, header: Boolean = false): DataFrame = {

    var options = sql_context.read.format("csv").option("header", header)
    options = options.option("inferSchema", true);

    return options.load(path);
  }

}