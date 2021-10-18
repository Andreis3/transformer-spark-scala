package com.softminded.pub.spark.transformer.transformers

import com.softminded.pub.spark.transformer.AwsGlueDDL._
import com.softminded.pub.spark.transformer.DataFrameTransformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LineItemDataFrameTransformer {

  def getSchema(tableName: String = "lineitem", location: String = "/tmp/lineitem") = Table(tableName,
    StorageDescriptor(
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", List(),
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", Compressed = false,
      SerdeInfo("org.apache.hadoop.hive.ql.io.orc.OrcSerde", Map()), location, -1, List(), Map(), StoredAsSubDirectories = false,
      List(
        TableColumn("l_orderkey", LongType),
        TableColumn("l_partkey", LongType),
        TableColumn("l_suppkey", LongType),
        TableColumn("l_linenumber", IntegerType),
        TableColumn("l_quantity", DecimalType.SYSTEM_DEFAULT),
        TableColumn("l_extendedprice", DecimalType.SYSTEM_DEFAULT),
        TableColumn("l_discount", DecimalType.SYSTEM_DEFAULT),
        TableColumn("l_tax", DecimalType.SYSTEM_DEFAULT),
        TableColumn("l_returnflag", StringType),
        TableColumn("l_linestatus", StringType),
        TableColumn("l_shipdate", DateType),
        TableColumn("l_commitdate", DateType),
        TableColumn("l_receiptdate", DateType),
        TableColumn("l_shipinstruct", StringType),
        TableColumn("l_shipmode", StringType),
        TableColumn("l_comment", StringType)
      )
    ),
    List())
}

class LineItemDataFrameTransformer extends DataFrameTransformer {

  override def transform(inputDF: DataFrame): DataFrame = {

    // select columns and cast each of them to an appropriate type
    val selectCols = LineItemDataFrameTransformer.getSchema().StorageDescriptor.Columns.map(
      tableCol => col(tableCol.Name).cast(tableCol.Type)
    )

    // transform input data frame
    inputDF.
      select(selectCols: _*)
  }

}
