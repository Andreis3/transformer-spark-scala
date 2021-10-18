package com.softminded.pub.spark.transformer.readers

import java.io.File
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.softminded.pub.spark.transformer._
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should.Matchers.be

class DataFrameCsvFileReaderSpec
  extends AnyFunSuiteLike
    with MockitoSugar
    with DataFrameComparer
    with SparkTestHelper {

  test("reads lineitem CSV with custom delimiter from a file") {

    withLocalSparkContext(spark => {

      import spark.implicits._

      // given: a CSV file and a data frame reader
      val file = new File(this.getClass.getClassLoader.getResource("testdata/tpch-0.001/data/lineitem.tbl").getFile)
      val reader = new DataFrameCsvFileReader(spark, DataFrameCsvFileReader.Config(file, '|'))

      // when: the file is read into a data frame
      val dataFrame = reader.read()

      // then: the data frame is read correctly
      dataFrame.count() should be(6)

      val expectedDF = spark.sparkContext.parallelize(Seq(("1", "1", "1", "1", "17", "15317.00", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"))).toDF()
      assertSmallDataFrameEquality(dataFrame.limit(1), expectedDF, ignoreColumnNames = true)
    })
  }

}
