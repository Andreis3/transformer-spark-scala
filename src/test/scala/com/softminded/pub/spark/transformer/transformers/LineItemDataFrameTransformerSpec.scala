package com.softminded.pub.spark.transformer.transformers

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.Date
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.softminded.pub.spark.transformer.AwsGlueDDL.{SerdeInfo, StorageDescriptor, Table}
import com.softminded.pub.spark.transformer.SparkTestHelper
import com.softminded.pub.spark.transformer.readers.DataFrameTableReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should._

class LineItemDataFrameTransformerSpec
  extends AnyFunSuiteLike
    with MockitoSugar
    with DataFrameComparer
    with SparkTestHelper {

  test("transforms lineitem from CSV to ORC") {

    withLocalSparkContext(spark => {

      import spark.implicits._

      withTempDir(s"${this.getClass.getName}-transform-to-orc")((tempDir: File) => {

        // given: data frame containing line item data read from CSV files in a folder
        val csvFile = new File(this.getClass.getClassLoader.getResource("testdata/tpch-0.001/data/lineitem.tbl").getFile)
        val csvFileCopy1 = Files.createTempFile(Paths.get(tempDir.getAbsolutePath), csvFile.getName, "-1.csv").toFile
        Files.copy(csvFile.toPath, csvFileCopy1.toPath, StandardCopyOption.REPLACE_EXISTING)

        val hiveTableName = "hive_lineitem_csv"
        val hiveTableLocation = tempDir.getAbsolutePath
        val hiveTableSchema = Table(hiveTableName,
          StorageDescriptor(
            null, List(),
            null, Compressed = false,
            SerdeInfo("org.apache.hadoop.hive.serde2.OpenCSVSerde", Map("separatorChar" -> "|")),
            hiveTableLocation, -1, List(), Map(), StoredAsSubDirectories = false,
            LineItemDataFrameTransformer.getSchema(hiveTableName, hiveTableLocation).StorageDescriptor.Columns
          ),
          List())

        withHiveTable(spark, hiveTableSchema) {

          val reader = new DataFrameTableReader(spark, DataFrameTableReader.Config(hiveTableName))
          val inputDF = reader.read()

          // when: the data is transformed
          val transformer = new LineItemDataFrameTransformer()
          val transformedDF = transformer.transform(inputDF)

          // then: transformed data frame contains correct, properly typed data
          transformedDF.count() should be(6)

          val expectedDF = spark.sparkContext.parallelize(Seq((
            1L, 1L, 1L, 1, BigDecimal(17), BigDecimal(15317.00), BigDecimal(0.04), BigDecimal(0.020),
            "N", "O", Date.valueOf("1996-03-13"), Date.valueOf("1996-02-12"), Date.valueOf("1996-03-22"),
            "DELIVER IN PERSON", "TRUCK", "egular courts above the"
          ))).toDF()
          assertSmallDataFrameEquality(transformedDF.limit(1), expectedDF, ignoreColumnNames = true, ignoreNullable = true)

        }
      })
    })
  }

  test("creates lineitem table with correct schema") {

    withLocalSparkContext(spark => {

      import spark.implicits._

      withTempDir(s"${this.getClass.getName}-output-schema")((tempDir: File) => {
        // given: lineitem table schema
        val schema = LineItemDataFrameTransformer.getSchema("lineitem", tempDir.getAbsolutePath)

        // when: a Hive table with this schema is created
        withLocalSparkContext(spark => {
          withHiveTable(spark, schema) {

            // then: the table has correct schema
            val storedDF = spark.sql(s"select * from ${schema.Name}")
            val expectedSchema = StructType(List(
              StructField("l_orderkey", LongType),
              StructField("l_partkey", LongType),
              StructField("l_suppkey", LongType),
              StructField("l_linenumber", IntegerType),
              StructField("l_quantity", DecimalType.SYSTEM_DEFAULT),
              StructField("l_extendedprice", DecimalType.SYSTEM_DEFAULT),
              StructField("l_discount", DecimalType.SYSTEM_DEFAULT),
              StructField("l_tax", DecimalType.SYSTEM_DEFAULT),
              StructField("l_returnflag", StringType),
              StructField("l_linestatus", StringType),
              StructField("l_shipdate", DateType),
              StructField("l_commitdate", DateType),
              StructField("l_receiptdate", DateType),
              StructField("l_shipinstruct", StringType),
              StructField("l_shipmode", StringType),
              StructField("l_comment", StringType)))

            val expectedDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], expectedSchema)
            assertSmallDataFrameEquality(storedDF, expectedDF)

            storedDF.count() should be(0)
          }
        })
      })
    })
  }
}
