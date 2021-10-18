package com.softminded.pub.spark.transformer

import org.apache.spark.sql.DataFrame

trait DataFrameTransformer {
  /**
    * Transforms the data from the given inputDF and returns the data transformed.
    *
    * @param inputDF data that needs to be transformed
    * @return transformed data
    */
  def transform(inputDF: DataFrame): DataFrame
}
