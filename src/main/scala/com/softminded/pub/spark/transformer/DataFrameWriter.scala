package com.softminded.pub.spark.transformer

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {
  /**
    * Writes the given dataframe to a target data source.
    * The target data source of is an implementation detail of the implementing class.
    *
    * @param df the dataframe to write
    */
  def write(df: DataFrame): Unit
}
