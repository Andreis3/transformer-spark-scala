package com.softminded.pub.spark.transformer.readers

import com.softminded.pub.spark.transformer.DataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameTableReader {

  case class Config(tableName: String)

}

class DataFrameTableReader(spark: SparkSession, config: DataFrameTableReader.Config) extends DataFrameReader {

  override def read(): DataFrame = {
    spark.sql(s"select * from ${config.tableName}")
  }

}
