package system

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import domain.Parquet

object Parameters {



  val path_activity = "C:\\sparkProjects\\Milano\\dataset\\*"
  val path_pc = "./dataset/pc/*"
  val path_printer = "./dataset/printer/*"
  val path_product = "./dataset/product/*"

  val table_activity= "parquet"
  val table_pc = "pc"
  val table_printer = "printer"
  val table_product = "product"

  private def createTable(name: String, structType: StructType, path: String, delimiter: String = "\\t")
                         (implicit spark: SparkSession): Unit = {
    spark.read
      //.format("com.databricks.spark.csv")
      //.option("inferSchema", "true")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N"
        )
      ).schema(structType).csv(path).createOrReplaceTempView(name)
  }

  def initTables(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_activity, Parquet.structType, Parameters.path_activity)

  }
}
