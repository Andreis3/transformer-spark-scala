package com.softminded.pub.spark.transformer

import com.softminded.pub.spark.transformer.AwsGlueDDL.SortColumn
import org.apache.spark.sql.types.StringType
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

class AwsGlueDDLSpec
  extends AnyFunSpecLike {

  describe("handles AWS Glue table schema") {

    it("parses AWS Glue table schema") {

      // given: the Glue schema
      val glueJson =
        """{
          |  "Name": "prsmessage",
          |  "StorageDescriptor": {
          |    "OutputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
          |    "SortColumns": [{
          |      "Column": "threat",
          |      "SortOrder": 1
          |    }],
          |    "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
          |    "Compressed": false,
          |    "SerdeInfo": {
          |      "SerializationLibrary": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
          |      "Parameters": {
          |        "serialization.format": "1"
          |      }
          |    },
          |    "Location": "s3://test-bucket/test-key/",
          |    "NumberOfBuckets": -1,
          |    "BucketColumns": [],
          |    "Parameters": {},
          |    "StoredAsSubDirectories": false,
          |    "Columns": [
          |      {
          |        "Name": "type",
          |        "Type": "string"
          |      },
          |      {
          |        "Name": "uuid",
          |        "Type": "string"
          |      }
          |    ]
          |  }
          |}""".stripMargin

      // when: the schema is parsed
      val table = AwsGlueDDL.tableFromJson(glueJson)

      // then: schema attributes match
      table.Name should equal("prsmessage")
      table.StorageDescriptor.OutputFormat should equal("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
      table.StorageDescriptor.InputFormat should equal("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
      table.StorageDescriptor.Compressed should equal(false)
      table.StorageDescriptor.SerdeInfo.SerializationLibrary should equal("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
      table.StorageDescriptor.SerdeInfo.Parameters should equal(Map[String, String]("serialization.format" -> "1"))
      table.StorageDescriptor.Location should equal("s3://test-bucket/test-key/")
      table.StorageDescriptor.NumberOfBuckets should equal(-1)
      table.StorageDescriptor.BucketColumns should equal(Array())
      table.StorageDescriptor.Parameters should equal(Map())
      table.StorageDescriptor.SortColumns should equal(List(SortColumn("threat", 1)))

      val structType = AwsGlueDDL.getSparkSchema(table)
      structType.fields(0).name should equal("type")
      structType.fields(0).dataType should equal(StringType)
      structType.fields(1).name should equal("uuid")
      structType.fields(1).dataType should equal(StringType)
    }
  }
}
