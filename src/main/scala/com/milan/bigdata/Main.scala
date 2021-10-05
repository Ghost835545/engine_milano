package com.milan.bigdata

import com.milan.bigdata.processing.InitTables
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql-labs")
      .getOrCreate()


    InitTables.initTables(spark)

    val zones = spark.table(InitTables.table_parquet)
      .sqlContext
      //Замена Null на 0.0
      .sql("Select square_id, " +
        "time_interval, " +
        "(coalesce(sms_in_activity,0) + coalesce(sms_out_activity,0) + coalesce(call_in_activity, 0) " +
        "+ coalesce(call_out_activity, 0) + coalesce(internet_traffic_activity, 0) ) activity " +
        "from parquet where country_code = 39 ")
    /*.withColumn("time",
      to_utc_timestamp(from_unixtime(col("time_interval") / 1000, "yyyy-MM-dd hh:mm:ss"), "Europe/Milan"))
    .withColumn("month", date_format(to_utc_timestamp(from_unixtime(col("time_interval") / 1000, "yyyy-MM-dd hh:mm:ss"), "Europe/Milan"), "LLLL"))
    .select(col("square_id"), col("time"), col("month"), col("activity"),
     expr("sum(activity) over(Partition by square_id, month)"),
      expr("CASE " +
      "WHEN ((date_format(time,'HH:mm:ss')>='00:00:00' AND date_format(time,'HH:mm:ss') < '09:00:00') " +
      "OR  (date_format(time,'HH:mm:ss') >= '17:00:00' AND date_format(time,'HH:mm:ss') < '23:59:59') " +
      "OR  (date_format(time,'HH:mm:ss') >= '13:00:00' AND date_format(time,'HH:mm:ss') < '15:00:00') " +
      "OR  (date_format(time,'HH:mm:ss') >= '09:00:00' AND date_format(time,'HH:mm:ss') < '13:00:00')" +
      "OR  (date_format(time,'HH:mm:ss') >= '15:00:00' AND date_format(time,'HH:mm:ss') < '17:00:00')) " +
      "AND (activity < avg(activity) over()) THEN 'Прогулочная зона' " +
      "WHEN ((date_format(time,'HH:mm:ss')>='00:00:00' AND date_format(time,'HH:mm:ss') < '09:00:00') " +
      "OR  (date_format(time,'HH:mm:ss') >= '17:00:00' AND date_format(time,'HH:mm:ss') < '23:59:59') " +
      "OR  (date_format(time,'HH:mm:ss') >= '13:00:00' AND date_format(time,'HH:mm:ss') < '15:00:00')) " +
      "AND (activity > avg(activity) over()) " +
      "OR  ((date_format(time,'HH:mm:ss') >= '09:00:00' AND date_format(time,'HH:mm:ss') < '13:00:00')" +
      "OR  (date_format(time,'HH:mm:ss') >= '15:00:00' AND date_format(time,'HH:mm:ss') < '17:00:00')) " +
      "AND (activity < avg(activity) over()) THEN 'Жилая зона' " +
      "WHEN ((" +
      "date_format(time,'HH:mm:ss')>='00:00:00' AND date_format(time,'HH:mm:ss') < '09:00:00') " +
      "OR  (date_format(time,'HH:mm:ss') >= '17:00:00' AND date_format(time,'HH:mm:ss') < '23:59:59') " +
      "OR  (date_format(time,'HH:mm:ss') >= '13:00:00' AND date_format(time,'HH:mm:ss') < '15:00:00')) " +
      "AND (activity < avg(activity) over()) " +
      "OR  ((date_format(time,'HH:mm:ss') >= '09:00:00' AND date_format(time,'HH:mm:ss') < '13:00:00') " +
      "OR  (date_format(time,'HH:mm:ss') >= '15:00:00' AND date_format(time,'HH:mm:ss') < '17:00:00')) " +
      "AND (activity > avg(activity) over()) THEN 'Рабочая зона' " +
      "END square_ind "
      ).as("zone"))
    .withColumnRenamed("sum(activity) OVER (PARTITION BY square_id, month UnspecifiedFrame)","sum_activity")
    .groupBy(col("square_id"), col("month"), col("sum_activity"), col("zone")).count()
    .select(col("square_id"), col("month"), col("sum_activity"), col("zone"))

  /*val toArray = udf[Array[String], String](_.split(","))

  val concatArray = udf((value: Seq[Seq[Seq[Double]]]) => {
    value.flatten.flatten.mkString(",")
  })

  val zip = udf((xs: Seq[Long], ys: Seq[Seq[Seq[Seq[Double]]]]) => xs.zip(ys))


  val coordinates = d.select(col("coordinates"), col("cellId"))
    .withColumn("coord", explode(zip(col("cellId"), col("coordinates"))))
    .select(col("coord._1").as("cellId"), concatArray(col("coord._2")).as("coordinat"))
    .select(col("cellId"), toArray(col("coordinat")).as("crs"))
    .select(col("cellId"),
      col("crs").getItem(0).substr(0, 7).as("lon1"), col("crs").getItem(1).substr(0, 8).as("lat1"),
      col("crs").getItem(2).substr(0, 7).as("lon2"), col("crs").getItem(3).substr(0, 8).as("lat2"),
      col("crs").getItem(4).substr(0, 7).as("lon3"), col("crs").getItem(5).substr(0, 8).as("lat3"),
      col("crs").getItem(6).substr(0, 7).as("lon4"), col("crs").getItem(7).substr(0, 8).as("lat4"),
      col("crs").getItem(8).substr(0, 7).as("lon5"), col("crs").getItem(9).substr(0, 8).as("lat5")
    )
  val zones_map = zones.join(coordinates, coordinates.col("cellID") === zones.col("square_id"))
    .select(col("square_id"), col("month"), col("time"),
      col("lat1"), col("lon1"), col("lat2"), col("lon2"),
      col("lat3"), col("lon3"), col("lat4"), col("lon4"),
      col("lat5"), col("lon5"))
  /*.coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv("./output/zones_map")*/

  InitTables.initTablePollutionLegend(spark)
  val pl = spark.table("pollution_legend")
    .withColumnRenamed("sensor_id", "sensor_id_legend")
    .select("sensor_id_legend", "sensor_street_name", "sensor_lat", "sensor_long", "sensor_type", "uom")

  InitTables.initTablePollutionMI(spark)
  val pmi = spark.table("pollution_mi")

  val pmi_legend = pmi.join(pl, pmi("sensor_id") === pl("sensor_id_legend"), "inner")
    .select(col("sensor_id"), col("time_instant"), col("measurement"), col("sensor_street_name")
      .substr(0,8).as("sensor_long")
      , col("sensor_lat").substr(0, 8).as("sensor_lat"), col("sensor_long").substr(0, 8).as("sensor_long")
      , col("sensor_type"))
    .groupBy("sensor_id","sensor_lat","sensor_long","sensor_street_name","sensor_type")
    .sum("measurement")
    .withColumnRenamed("sum(measurement)","total_measurement")

  pmi_legend.crossJoin(coordinates)
    .filter(
      ((col("lat1") >= col("sensor_lat") && col("lat2") <= col("sensor_lat"))
        ||
        (col("lon1") <= col("sensor_long") && col("lon2") >= col("sensor_long")))
        ||
        ((col("lat2") >= col("sensor_lat") && col("lat3") <= col("sensor_long"))
        || (col("lon2")>=col("sensor_long") && col("lon3") >= col("sensor_long")))
        ||
        ((col("lat3") <= col("sensor_lat") && col("lat4") <= col("sensor_long")
        && col("lat1") >=col("sensor_lat"))
        || (col("lon3")>=col("sensor_long") && col("lon4")<=col("sensor_long")
        && col("lon1") <= col("sensor_long"))))*/*/
    spark.stop()
  }
}
