package system

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import domain.{Activity, Cross, Parquet, PollLegend, PollMi}

object Parameters {



  val path_parquet = "C:\\sparkProjects\\Milano\\dataset\\*"
  val path_pollution_legend_mi = "C:\\sparkProjects\\Milano\\pollution-legend-mi.csv"
  val path_pollution_mi = "C:\\sparkProjects\\Milano\\pollution-mi\\*"
  val path_activity = "./output/activity/"
  val path_result_activity = "./output/activity/activity.csv"
  val path_result_zone = "./output/zone"
  val path_result_legend="./output/legend"
  val path_result_map = "./output/map"

  val path_mi_grid = "C:\\sparkProjects\\Milano\\mi_grid\\*"
  val path_printer = "./dataset/printer/*"
  val path_product = "./dataset/product/*"

  val table_parquet= "parquet"
  val table_activity = "activity"
  val table_pollution_legend = "pollution_legend"
  val table_pollution_mi = "pollution_mi"

  val table_printer = "printer"
  val table_product = "product"

  private def createTable(name: String, structType: StructType, path: String, delimiter: String)
                         (implicit spark: SparkSession): Unit = {
    spark.read
      //.format("com.databricks.spark.csv")
      //.option("inferSchema", "true")
      .format("csv")
      .option("header", "false")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N"
        )
      ).schema(structType).load(path).createOrReplaceTempView(name)

  }

  def initTables(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_parquet, Parquet.structType, Parameters.path_parquet,"\\t")
  }

  def initTableActivity(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_activity, Activity.structType, Parameters.path_result_activity,"\\t")
  }
  def initTablePollutionLegend(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_pollution_legend, PollLegend.structType, Parameters.path_pollution_legend_mi,",")
  }
  def initTablePollutionMI(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_pollution_mi, PollMi.structType, Parameters.path_pollution_mi,",")
  }
  def initTableCross(implicit spark: SparkSession): Unit = {
    createTable("cross", Cross.structType, "./output/vars/cross.csv",",")
  }
}
