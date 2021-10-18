package com.softminded.pub.spark.transformer.transformers

import com.softminded.pub.spark.transformer.DataFrameTransformer
import org.apache.spark.sql.DataFrame

class IdentityDataFrameTransformer extends DataFrameTransformer {

  override def transform(inputDF: DataFrame): DataFrame = {
    inputDF
  }

}
