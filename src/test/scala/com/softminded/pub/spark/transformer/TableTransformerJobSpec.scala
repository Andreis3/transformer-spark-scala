package com.softminded.pub.spark.transformer

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.softminded.pub.spark.transformer.readers.DataFrameTableReader
import com.softminded.pub.spark.transformer.transformers.IdentityDataFrameTransformer
import com.softminded.pub.spark.transformer.writers.DataFrameTableWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike

class TableTransformerJobSpec
  extends AnyFunSuiteLike
    with MockitoSugar
    with DataFrameComparer
    with SparkTestHelper {

  private def newTableTransformerJob(spark: SparkSession, mockReader: DataFrameReader, mockWriter: DataFrameWriter, mockTransformer: DataFrameTransformer) = {
    val readerConfig = DataFrameTableReader.Config("inputTable")
    val writerConfig = DataFrameTableWriter.Config("outputName", "orc")
    val config = TableTransformerConfig("com.softminded.pub.spark.transformer.SomeTransformerClass", readerConfig, writerConfig)
    new TableTransformerJob(spark, config) {

      override protected def newTransformer(): DataFrameTransformer = mockTransformer

      override protected def newReader(): DataFrameReader = mockReader

      override protected def newWriter(): DataFrameWriter = mockWriter
    }
  }

  private def newTableTransformerJob(spark: SparkSession, mockReader: DataFrameReader, mockWriter: DataFrameWriter, transformerClass: String) = {
    val readerConfig = DataFrameTableReader.Config("inputTable")
    val writerConfig = DataFrameTableWriter.Config("outputName", "orc")
    val config = TableTransformerConfig(transformerClass, readerConfig, writerConfig)
    new TableTransformerJob(spark, config) {

      override protected def newReader(): DataFrameReader = mockReader

      override protected def newWriter(): DataFrameWriter = mockWriter
    }
  }

  test("TableTransformerJob calls reader, transformer and writer") {

    // given: mocked reader, writer and transformer
    val spark = mock[SparkSession]
    val mockReader = mock[DataFrameReader]
    val mockWriter = mock[DataFrameWriter]
    val mockTransformer = mock[DataFrameTransformer]

    val mockInputDF = mock[DataFrame]
    when(mockReader.read()).thenReturn(mockInputDF)

    val mockOutputDF = mock[DataFrame]
    when(mockTransformer.transform(mockInputDF)).thenReturn(mockOutputDF)

    // when: the job is run
    newTableTransformerJob(spark, mockReader, mockWriter, mockTransformer).run()

    // then: mocked reader, transformer and writer are called with the correct data frames
    verify(mockReader).read()
    verify(mockTransformer).transform(mockInputDF)
    verify(mockWriter).write(mockOutputDF)
  }

  test("TableTransformerJob delegates to the fake transformer") {

    // given: mocked reader and writer and fake transformerClass set in the job's configuration
    val spark = mock[SparkSession]
    val mockReader = mock[DataFrameReader]
    val mockWriter = mock[DataFrameWriter]

    val mockInputDF = mock[DataFrame]
    when(mockReader.read()).thenReturn(mockInputDF)

    // when: the job is run
    newTableTransformerJob(spark, mockReader, mockWriter, classOf[FakeDataFrameTransformer].getName).run()

    // then: mocked reader, transformer and writer are called with the correct data frames
    verify(mockWriter).write(FakeDataFrameTransformer.outputDF)
  }

  test("TableTransformerJob performs identity transformation correctly") {

    withLocalSparkContext(spark => {
      import spark.implicits._

      // given: some input data registered as a Spark table and a transformer configured to transform the input data to ORC format
      val inputDF = spark.sparkContext.parallelize(Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))).toDF("id", "date", "hour", "content")
      inputDF.createOrReplaceTempView("input_table")

      val readerConfig = DataFrameTableReader.Config("input_table")
      val writerConfig = DataFrameTableWriter.Config("output_table", "orc")
      val config = TableTransformerConfig(classOf[IdentityDataFrameTransformer].getName, readerConfig, writerConfig)

      // identity transformation should preserve input schema in the output
      val outputTableSchema = inputDF.schema

      withSparkTable(spark, writerConfig.tableName, outputTableSchema) {

        // when: the job is run
        new TableTransformerJob(spark, config).run()

        // then: identity transformation returns correct data
        val outputDF = spark.sql("select * from output_table")
        assertSmallDataFrameEquality(outputDF, inputDF, ignoreNullable = true)
      }
    })
  }
}
