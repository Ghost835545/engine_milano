package training

import domain.ActivityCase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, to_date, to_utc_timestamp, when}
import system.Parameters

import java.io.File



object App extends App{
val d = new File(Parameters.path_activity)
  val d1 = new File(Parameters.path_result_zone)

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
      "sum(sms_in_activity+sms_out_activity+call_in_activity+call_out_activity+internet_traffic_activity) activity" +
      " from (select square_id, time_interval," +
              " coalesce(sms_in_activity,0) sms_in_activity, " +
              "coalesce(sms_out_activity,0) sms_out_activity, " +
              "coalesce(call_in_activity, 0) call_in_activity, " +
              "coalesce(call_out_activity, 0) call_out_activity," +
              "coalesce(internet_traffic_activity, 0) internet_traffic_activity " +
                "from parquet where country_code = 39 ) t " +
      "GROUP by square_id, time_interval ")
      .withColumn("time",
       to_utc_timestamp(from_unixtime(col("time_interval")/1000,"yyyy-MM-dd hh:mm:ss"),"Europe/Milan"))
    .withColumn("month",date_format(to_utc_timestamp(from_unixtime(col("time_interval")/1000,"yyyy-MM-dd hh:mm:ss"),"Europe/Milan"),"LLLL"))
    .select("square_id","time","month","activity")
      .coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(Parameters.path_activity)


}
  else if (!d1.exists()){

    Parameters.initTableActivity(spark)
    import spark.implicits._

   val p = spark.table("activity")
     .sqlContext
      .sql("SELECT square_id, month, "+
       "CASE "+
        "WHEN ((date_format(time,'HH:mm:ss')>='00:00:00' AND date_format(time,'HH:mm:ss') < '09:00:00') "+
        "OR  (date_format(time,'HH:mm:ss') >= '17:00:00' AND date_format(time,'HH:mm:ss') < '23:59:59') "+
        "OR  (date_format(time,'HH:mm:ss') >= '13:00:00' AND date_format(time,'HH:mm:ss') < '15:00:00') "+
        "OR  (date_format(time,'HH:mm:ss') >= '09:00:00' AND date_format(time,'HH:mm:ss') < '13:00:00')"+
        "OR  (date_format(time,'HH:mm:ss') >= '15:00:00' AND date_format(time,'HH:mm:ss') < '17:00:00')) "+
        "AND (activity < avg(activity) over()) THEN 'Прогулочная зона' "+
    "WHEN ((date_format(time,'HH:mm:ss')>='00:00:00' AND date_format(time,'HH:mm:ss') < '09:00:00') "+
      "OR  (date_format(time,'HH:mm:ss') >= '17:00:00' AND date_format(time,'HH:mm:ss') < '23:59:59') "+
      "OR  (date_format(time,'HH:mm:ss') >= '13:00:00' AND date_format(time,'HH:mm:ss') < '15:00:00')) "+
      "AND (activity > avg(activity) over()) " +
        "OR  ((date_format(time,'HH:mm:ss') >= '09:00:00' AND date_format(time,'HH:mm:ss') < '13:00:00')"+
        "OR  (date_format(time,'HH:mm:ss') >= '15:00:00' AND date_format(time,'HH:mm:ss') < '17:00:00')) "+
        "AND (activity < avg(activity) over()) THEN 'Жилая зона' "+
        "WHEN ((" +
        "date_format(time,'HH:mm:ss')>='00:00:00' AND date_format(time,'HH:mm:ss') < '09:00:00') "+
        "OR  (date_format(time,'HH:mm:ss') >= '17:00:00' AND date_format(time,'HH:mm:ss') < '23:59:59') "+
        "OR  (date_format(time,'HH:mm:ss') >= '13:00:00' AND date_format(time,'HH:mm:ss') < '15:00:00')) "+
        "AND (activity < avg(activity) over()) " +
        "OR  ((date_format(time,'HH:mm:ss') >= '09:00:00' AND date_format(time,'HH:mm:ss') < '13:00:00') "+
        "OR  (date_format(time,'HH:mm:ss') >= '15:00:00' AND date_format(time,'HH:mm:ss') < '17:00:00')) "+
        "AND (activity > avg(activity) over()) THEN 'Рабочая зона' "+
        "END square_ind "+
        "FROM activity")
     .coalesce(1)
     .write
     .option("header", "false")
     .option("delimiter", "\t")
     .mode("overwrite")
     .csv(Parameters.path_result_zone)



  }
  else {
    val mi_grid = spark.read.json(Parameters.path_mi_grid)
    mi_grid.show()
  }

  spark.stop()
}
