package com.softminded.pub.spark.transformer

import org.apache.spark.sql.DataFrame
import org.mockito.MockitoSugar

object FakeDataFrameTransformer extends MockitoSugar {
  val outputDF: DataFrame = mock[DataFrame]
}

class FakeDataFrameTransformer extends DataFrameTransformer {
  override def transform(inputDF: DataFrame): DataFrame = {
    FakeDataFrameTransformer.outputDF
  }
}
