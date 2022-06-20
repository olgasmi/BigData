package agh.wggios.analizadanych.datawriter
import org.apache.spark.sql.DataFrame
import sbt.util.LogExchange.logger

class DataWriter(){

  def write(df:DataFrame,path:String, format: String = "csv"): Unit = {
    df.write.format(format).save(path)
    logger.info("Saving dataframe")
  }

}
