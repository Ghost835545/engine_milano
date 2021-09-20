package utility

import org.apache.spark.sql.SparkSession

object Utls {

  def saveDF(spark: SparkSession, table_name: String, path: String) =
    spark.table(table_name)
      .coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv("./output/activity/")

  def deleteNull(spark: SparkSession, table_name:String)=
    spark.table(table_name)
      .sqlContext
      //Замена Null на 0.0
      .sql("select square_id, time_interval, " +
        "coalesce(sms_in_activity,0) sms_in_activity, " +
        "coalesce(sms_out_activity,0) sms_out_activity, " +
        "coalesce(call_in_activity, 0) call_in_activity, " +
        "coalesce(call_out_activity, 0) call_out_activity," +
        " coalesce(internet_traffic_activity, 0) internet_traffic_activity   " +
        " from parquet where country_code = 39")
}
