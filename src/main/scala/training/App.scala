package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import system.Parameters


object App extends App{

  private val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark-sql-labs")
    .getOrCreate()

  Parameters.initTables(spark)

  val pc = spark.table("parquet").show()

  spark.stop()


}
