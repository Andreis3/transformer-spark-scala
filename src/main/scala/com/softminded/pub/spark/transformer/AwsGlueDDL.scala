package com.softminded.pub.spark.transformer

import java.io.InputStream

import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{JsonInput, StreamInput, StringInput}

object AwsGlueDDL {

  case class Table(Name: String, StorageDescriptor: StorageDescriptor, PartitionKeys: List[TablePartitionKey])

  case class SerdeInfo(SerializationLibrary: String, Parameters: Map[String, String])

  object TableColumn {
    def apply(Name: String, Type: DataType) = new TableColumn(Name, Type.typeName, None, None)
  }

  case class TableColumn(Name: String, Type: String, Comment: Option[String], Parameters: Option[Map[String, String]])

  case class TablePartitionKey(Name: String, Type: String, Comment: Option[String], parameters: Option[Map[String, String]])

  case class SortColumn(Column: String, SortOrder: Int) {
    require(Array(0, 1) contains SortOrder, "SortOrder must be 0 or 1 - for descending or ascending order respectively")

    def getSqlOrder: String = {
      if (SortOrder == 0) {
        "desc"
      }
      else {
        "asc"
      }
    }
  }

  case class StorageDescriptor(OutputFormat: String, SortColumns: List[SortColumn], InputFormat: String, Compressed: Boolean,
    SerdeInfo: SerdeInfo, Location: String, NumberOfBuckets: Integer, BucketColumns: List[String], Parameters: Map[String, String],
    StoredAsSubDirectories: Boolean, Columns: List[TableColumn])

  def tableFromJson(jsonStream: String): Table = {
    tableFromJson(StringInput(jsonStream))
  }

  def tableFromJson(jsonStream: InputStream): Table = {
    tableFromJson(StreamInput(jsonStream))
  }

  private def tableFromJson(input: JsonInput) = {
    //noinspection TypeAnnotation
    implicit val formats = org.json4s.DefaultFormats //+ new StorageDescriptorSerializer

    parse(input).extract[Table]
  }

  def getSparkSchema(table: Table): StructType = {
    var schema = new StructType()
    for (column <- table.StorageDescriptor.Columns) {
      assert(!column.Name.contains("`"), s"Column name ${column.Name} should not contain backticks")
      assert(!column.Type.contains("`"), s"Column type ${column.Type} should not contain backticks")
      schema = schema.add(column.Name, DataType.fromDDL(column.Type))
    }
    schema
  }
}
