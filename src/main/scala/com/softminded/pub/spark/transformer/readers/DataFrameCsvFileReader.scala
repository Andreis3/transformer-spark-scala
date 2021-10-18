package com.softminded.pub.spark.transformer.readers

import java.io.File

import com.softminded.pub.spark.transformer.DataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameCsvFileReader {

  case class Config(file: File, separator: Char, hasHeader: Boolean = false)

}

class DataFrameCsvFileReader(spark: SparkSession, config: DataFrameCsvFileReader.Config) extends DataFrameReader {

  override def read(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .csv(config.file.getAbsolutePath)
  }

}
