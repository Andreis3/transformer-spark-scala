package com.softminded.pub.spark.transformer

import java.io.File
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.mockito.MockitoSugar

trait SparkTestHelper extends MockitoSugar with LazyLogging {

  private val spark = SparkSession.builder()
    .appName("spark testing")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  /**
    * Based on the loan pattern from https://stackoverflow.com/questions/43729262/how-to-write-unit-tests-in-spark-2-0
    */
  def withMockSparkContext(testFunction: SparkSession => Any) {
    val spark = mock[SparkSession]
    testFunction(spark)
  }

  def withLocalSparkContext(testFunction: SparkSession => Any) {

    try {
      testFunction(spark)
    }
    finally {
      // not closing to allow the session to be reused to speed up tests
      // spark.stop()
    }
  }

  def withTempDir(dirNamePrefix: String)(testFunction: File => Any) {
    val tempDir = Files.createTempDirectory(dirNamePrefix).toFile
    try {
      testFunction(tempDir)
    }
    finally {
      tempDir.deleteOnExit()
    }
  }

  def withSparkTable(spark: SparkSession, tableName: String, tableSchema: StructType)(testFunction: => Any) {
    try {
      val sql = s"create table $tableName(${tableSchema.toDDL})"
      logger.info(s"About to create table: $sql")
      spark.sql(sql)

      testFunction
    }
    finally {
      spark.sql(s"drop table if exists $tableName")
    }
  }

  private def getSerdePropertiesHiveClause(serdeInfo: AwsGlueDDL.SerdeInfo): String = {
    if (serdeInfo.Parameters.isEmpty) {
      ""
    }
    else {
      serdeInfo.Parameters.map { case (k, v) => s""""$k"="$v"""" }.mkString("WITH SERDEPROPERTIES (", ",", ")")
    }
  }

  def withHiveTable(spark: SparkSession, tableSchema: AwsGlueDDL.Table)(testFunction: => Any) {
    val serDeLib = tableSchema.StorageDescriptor.SerdeInfo.SerializationLibrary
    require(Class.forName(serDeLib) != null, s"Failed to load ${tableSchema.StorageDescriptor.SerdeInfo.SerializationLibrary} serde class")

    val serDeProps = getSerdePropertiesHiveClause(tableSchema.StorageDescriptor.SerdeInfo)
    val colList = tableSchema.StorageDescriptor.Columns.map(column => column.Name + " " + column.Type).mkString(",")
    val partitionColList = tableSchema.PartitionKeys.map(column => column.Name + " " + column.Type).mkString(",")
    val inputFormatClause = Option(tableSchema.StorageDescriptor.InputFormat).map(t => s"STORED AS INPUTFORMAT '$t'").getOrElse("")
    val outputFormatClause = Option(tableSchema.StorageDescriptor.OutputFormat).map(t => s"OUTPUTFORMAT '$t'").getOrElse("")

    try {
      var sql =
        s"""
           |CREATE EXTERNAL TABLE ${tableSchema.Name}($colList)
           |ROW FORMAT serde "$serDeLib" $serDeProps
           |$inputFormatClause
           |$outputFormatClause
           |LOCATION "${tableSchema.StorageDescriptor.Location}"
        """.stripMargin

      if (!partitionColList.isEmpty) {
        sql += s" PARTITIONED BY ($partitionColList)"
      }

      logger.info(s"About to create table: $sql")
      spark.sql(sql)

      testFunction
    }
    finally {
      spark.sql(s"drop table if exists ${tableSchema.Name}")
    }
  }

}
