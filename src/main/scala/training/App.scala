package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, to_utc_timestamp}
import system.Parameters
 import java.io.File
import utility.Utls



object App extends App{
val d = new File(Parameters.path_activity)
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark-sql-labs")
    .getOrCreate()


  if (!d.exists()){


  Parameters.initTables(spark)

    val df = spark.table(Parameters.table_parquet)
      .sqlContext
      //Замена Null на 0.0
    .sql("Select square_id, " +
      "time_interval,  " +
      "avg(sms_in_activity+sms_out_activity+call_in_activity+call_out_activity+internet_traffic_activity) over() avg_activity," +
      "sum(sms_in_activity+sms_out_activity+call_in_activity+call_out_activity+internet_traffic_activity) over(PARTITION BY square_id, time_interval) activity" +
      " from (select square_id, time_interval," +
              " coalesce(sms_in_activity,0) sms_in_activity, " +
              "coalesce(sms_out_activity,0) sms_out_activity, " +
              "coalesce(call_in_activity, 0) call_in_activity, " +
              "coalesce(call_out_activity, 0) call_out_activity," +
              "coalesce(internet_traffic_activity, 0) internet_traffic_activity " +
                "from parquet where country_code = 39 ) t")
     .withColumn("time",
       date_format(to_utc_timestamp(from_unixtime(col("time_interval")/1000,"yyyy-MM-dd hh:mm:ss"),"Europe/Milan"),"hh:mm:ss"))
    .withColumn("month",date_format(to_utc_timestamp(from_unixtime(col("time_interval")/1000,"yyyy-MM-dd hh:mm:ss"),"Europe/Milan"),"LLLL"))
    .select("square_id","time","month","avg_activity","activity")
      .coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(Parameters.path_activity)


}
  else {
    Parameters.initTableActivity(spark)
   spark.table("activity").show()
  }





  spark.stop()
}
