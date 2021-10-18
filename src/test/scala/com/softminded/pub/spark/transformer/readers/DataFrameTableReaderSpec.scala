package com.softminded.pub.spark.transformer.readers

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.softminded.pub.spark.transformer.AwsGlueDDL.{SerdeInfo, StorageDescriptor, Table, TableColumn}
import com.softminded.pub.spark.transformer._
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataFrameTableReaderSpec
  extends AnyFunSuiteLike
    with MockitoSugar
    with DataFrameComparer
    with SparkTestHelper {

  test("reads lineitem CSV with custom delimiter from a folder") {

    withLocalSparkContext(spark => {

      import spark.implicits._

      withTempDir(s"${this.getClass.getName}-read-folder")((tempDir: File) => {

        // given: a folder with CSV files and a Hive table on top of that folder
        val csvFile = new File(this.getClass.getClassLoader.getResource("testdata/tpch-0.001/data/lineitem.tbl").getFile)
        val csvFileCopy1 = Files.createTempFile(Paths.get(tempDir.getAbsolutePath), csvFile.getName, "-1.csv").toFile
        Files.copy(csvFile.toPath, csvFileCopy1.toPath, StandardCopyOption.REPLACE_EXISTING)
        val csvFileCopy2 = Files.createTempFile(Paths.get(tempDir.getAbsolutePath), csvFile.getName, "-2.csv").toFile
        Files.copy(csvFile.toPath, csvFileCopy2.toPath, StandardCopyOption.REPLACE_EXISTING)

        val hiveTableName = "hive_lineitem_csv"
        val hiveTableLocation = tempDir.getAbsolutePath
        val hiveTableSchema = Table(hiveTableName,
          StorageDescriptor(
            null, List(),
            null, Compressed = false,
            SerdeInfo("org.apache.hadoop.hive.serde2.OpenCSVSerde", Map("separatorChar" -> "|")),
            hiveTableLocation, -1, List(), Map(), StoredAsSubDirectories = false,
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

        withHiveTable(spark, hiveTableSchema) {

          // when: the table is read into a data frame
          val reader = new DataFrameTableReader(spark, DataFrameTableReader.Config(hiveTableName))
          val dataFrame = reader.read()

          // then: the data frame is read correctly
          dataFrame.count() should be(12)

          val expectedDF = spark.sparkContext.parallelize(Seq(("1", "1", "1", "1", "17", "15317.00", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"))).toDF()
          assertSmallDataFrameEquality(dataFrame.limit(1), expectedDF, ignoreColumnNames = true)

        }
      })
    })
  }

}
