package com.softminded.pub.spark.transformer

import org.apache.spark.sql.DataFrame

trait DataFrameReader {
  /**
    * Reads data into a dataframe and returns the dataframe.
    * The source of the data is an implementation detail of the implementing class.
    *
    * @return the data read
    */
  def read(): DataFrame
}
