package com.milan.bigdata

import com.milan.bigdata.processing.InitTables
import com.milan.bigdata.system.Parameters
import org.apache.calcite.sql.`type`.SqlOperandCountRanges.between
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{abs, array, avg, coalesce, col, count, date_format, explode, expr, from_unixtime, lit, split, sum, to_date, to_utc_timestamp, udf, when}

object Main {

  def main(args: Array[String]): Unit = {
    implicit val conf: Parameters = new Parameters(args)

    implicit val spark = SparkSession
      .builder()
      /* расширение, позволяющее использовать в таблицах crossJoin*/
      .config("spark.sql.crossJoin.enabled", "true")
       .appName("milan_task")
      .master("local[*]")
      //.enableHiveSupport()
      //.config("hive.exec.dynamic.partition","true")
      //.config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    InitTables.initTables(spark)

    val activity_sum = spark.table(InitTables.table_parquet)
      //.sqlContext
      /* Выборка общей активности, даты */
      /*.sql("Select square_id, " +
        " to_date(cast(time_interval/1000 AS TIMESTAMP)) AS date_activity, " +
        "cast(cast(time_interval/1000 AS TIMESTAMP) AS BIGINT) AS datetime_activity, " +
        "(nvl(sms_in_activity,0) + nvl(sms_out_activity,0) + nvl(call_in_activity, 0) " +
        "+ nvl(call_out_activity, 0) + nvl(internet_traffic_activity, 0) ) activity,  " +
        "country_code " +
        "from parquet ")*/
      .select(
        col("square_id"),
        to_date((col("time_interval")/1000).cast("TIMESTAMP"),"yyyy-MM-dd").as("date_activity"),
        from_unixtime((col("time_interval")/1000).cast("TIMESTAMP").cast("BIGINT"),
          "yyyy-MM-dd hh:mm:ss").as("datetime"),
        date_format(to_utc_timestamp(from_unixtime(col("time_interval")/1000,
          "yyyy-MM-dd hh:mm:ss"), "Europe/Milan"), "LLLL").as("month"),
        col("country_code"),
        //date_format(to_utc_timestamp(from_unixtime(col("time_interval")/1000,
          //"yyyy-MM-dd hh:mm:ss"), "Europe/Milan"), "hh:mm").as("hh"),
        (coalesce(col("sms_in_activity"),lit(0)).as("sms_in_activity") +
          coalesce(col("sms_out_activity"),lit(0)).as("sms_out_activity") +
          coalesce(col("call_in_activity"),lit(0)).as("call_in_activity") +
          coalesce(col("call_out_activity"),lit(0)).as("call_out_activity")+
          coalesce(col("internet_traffic_activity"),lit(0)).as("internet_traffic_activity")).as("activity")
      )
   .createOrReplaceTempView("activities")
      /* Выборка средней активности в рабочее время и нерабочее время   */
    val activities =  spark.table("activities")
       /*.sqlContext
       .sql("SELECT " +
         "square_id, " +
         "month, " +
         "SUM (activity) AS sum_activity, " +
         "AVG( IF (datetime_activity - cast(cast(date_activity AS TIMESTAMP) AS BIGINT) BETWEEN 32400 AND 61200 , activity, 0) ) AS work_time_activity, " +
         "AVG( IF (datetime_activity - cast(cast(date_activity AS TIMESTAMP) AS BIGINT) NOT BETWEEN 32400 AND 61200 , activity, 0) ) AS not_work_time_activity,  " +
         "COUNT( IF (country_code = 39, 1, NULL) ) AS call_code_it, " +
         "COUNT( IF (country_code = 39, NULL, 1) )AS call_code_not_it " +
         "FROM activities " +
         "GROUP BY square_id, month, date_activity " +
         "ORDER BY square_id, month, date_activity")
        .createOrReplaceTempView("depend_activities")*/
      .select(
        col("square_id"),
        col("month"),
        col("date_activity"),
        col("activity"),
        col("datetime"),
        col("country_code")
      )
      .groupBy("square_id","month","date_activity","datetime")
      .agg(sum("activity"),
        avg(when(date_format(col("datetime"),"hh:mm").
          between("09:00","17:00"),col("activity")).otherwise(0)).as("work_time_activity"),
        avg(when(!date_format(col("datetime"),"hh:mm").between("09:00","17:00"),col("activity"))
          .otherwise(0)).as("not_work_time_activity"),
        count(when(col("country_code")===39,1).otherwise(0)).as("call_code_it"),
        count(when(col("country_code")===39,0).otherwise(2)).as("call_code_not_it")
      )
      .orderBy("square_id","month","date_activity")

      //.select("square_id","month","work_time_activity","not_work_time_activity","call_code_it","call_code_not_it")
    //.createOrReplaceTempView("depend_activities")

 val re = activities
   .select(
     col("square_id").as("s_id"),
     col("month").as("m"),
     col("date_activity").as("date_activit"),
     col("work_time_activity"),
     col("not_work_time_activity")
   )
   .groupBy("s_id","date_activit","m")
   .agg(avg(abs(col("work_time_activity")-col("not_work_time_activity"))).as("avg_all"))
   .orderBy("s_id","m", "date_activit").toDF()

    //val result = spark.table("depend_activities").toDF()

   val avg_activities =  activities
      .join(re,re("s_id")===activities("square_id"),"inner")
      .select("square_id","month","work_time_activity","not_work_time_activity","avg_all","call_code_it","call_code_not_it")
      .groupBy("square_id","month","work_time_activity","not_work_time_activity","avg_all","call_code_it","call_code_not_it").count()
      .orderBy("square_id","month").toDF()

val zones =
  avg_activities
    .select(
      col("square_id"),
      col("month"),
      col("work_time_activity"),
      col("not_work_time_activity"),
      col("call_code_it"),
      col("call_code_not_it"),
      col("avg_all"),
      abs(col("work_time_activity")-col("not_work_time_activity")).as("activity"),
      when(abs(col("work_time_activity")-col("not_work_time_activity"))<=col("avg_all") &&
      col("call_code_it")<col("call_code_not_it"),"туристическая зона")
        .otherwise(when(col("work_time_activity")>col("not_work_time_activity")||
          abs(col("work_time_activity")-col("not_work_time_activity"))<=col("avg_all"),"Рабочая зона")
          .otherwise("Жилая зона")).as("zone")
    )
    .groupBy("square_id","month","avg_all","zone").count()


  /*Выборка:  общая активность, рабочая_активность, нерабочая активность, телефонный код Милана, телефонный код не Милана */
 /*spark.table("depend_activities")
   .sqlContext
   .sql("SELECT " +
     "square_id, month," +
     "avg(call_code_it) AS avg_call_code_it, " +
     "avg(call_code_not_it) AS avg_call_code_not_it, " +
     "avg(work_time_activity) as avg_work_time_activity, " +
     "avg(not_work_time_activity) as avg_not_work_time_activity, " +
     "a.avg_diff_time_activity AS avg_all " +
     "from depend_activities " +
     "LEFT JOIN (SELECT avg(abs(work_time_activity - not_work_time_activity)) AS avg_diff_time_activity FROM depend_activities) a " +
     "GROUP BY square_id, month, avg_all")
 .createOrReplaceTempView("avg_activities")*/

    /*spark.table("depend_activities")
      .select(
        col("square_id"),
        col("month")).join("depend_activities",)*/

 /*Алгоритм разбияние на зоны: Туристическая, елси активность меньше средней и телефонный код не Милан,
 Рабочая - автивность в рабочее время больше,
 чем активность в нерабочее время, то рабочая зона, иначе жилая     */
/*val zones =  spark.table("avg_activities")
   .sqlContext
   .sql("SELECT " +
     "square_id, month, abs(avg_work_time_activity - avg_not_work_time_activity) as activity, " +
     "CASE " +
         "WHEN " +
     " abs(avg_work_time_activity - avg_not_work_time_activity) <= avg_all AND avg_call_code_it < avg_call_code_not_it " +
     "THEN 'туристическая зона' " +
         "WHEN " +
                 "avg_work_time_activity > avg_not_work_time_activity OR " +
     " abs(avg_work_time_activity - avg_not_work_time_activity) <= avg_all " +
     "THEN 'Рабочая зона' " +
     "ELSE " +
        " 'Жилая зона' " +
     "END as zone " +
     "FROM avg_activities " +
     "GROUP BY square_id, month, avg_call_code_it, avg_call_code_not_it, avg_work_time_activity, avg_not_work_time_activity, avg_all " +
     "ORDER BY square_id, month")*/
    /*
     */
   /*.coalesce(1)
   .write
   .option("header", "false")
   .option("delimiter", "\t")
   .mode("overwrite")
   .csv("./output/zones")*/

/*
*/

 val toArray = udf[Array[String], String](_.split(","))

 val concatArray = udf((value: Seq[Seq[Seq[Double]]]) => {
   value.flatten.flatten.mkString(",")
 })
val zip = udf((xs: Seq[Long], ys: Seq[Seq[Seq[Seq[Double]]]]) => xs.zip(ys))
   /* Извлечение координат сетки square_id*/
   val d =spark.read.json(InitTables.path_mi_grid).select("features.properties.cellID","features.geometry.coordinates" )
   val coordinates = d.select(col("coordinates"),col("cellId"))
     .withColumn("coord",explode(col("coordinates")))
     .select(
       col("coord").getItem(0).getItem(0).getItem(0).as("lat1"),
       col("coord").getItem(0).getItem(0).getItem(1).as("lon1"),
       col("coord").getItem(0).getItem(1).getItem(0).as("lat2"),
       col("coord").getItem(0).getItem(1).getItem(1).as("lon2"),
       col("coord").getItem(0).getItem(2).getItem(1).as("lat3"),
       col("coord").getItem(0).getItem(2).getItem(0).as("lon3"),
       col("coord").getItem(0).getItem(3).getItem(1).as("lat4"),
       col("coord").getItem(0).getItem(3).getItem(0).as("lon4"),
       col("coord").getItem(0).getItem(4).getItem(1).as("lat5"),
       col("coord").getItem(0).getItem(4).getItem(0).as("lon5")
     )



   /*.withColumn("coord", explode(zip(col("cellId"), col("coordinates"))))
   .select(col("coord._1").as("cellId"), concatArray(col("coord._2")).as("coordinat"))
   .select(col("cellId"), toArray(col("coordinat")).as("crs"))
   .select(col("cellId"),
     col("crs").getItem(0).substr(0, 7).as("lon1"), col("crs").getItem(1).substr(0, 8).as("lat1"),
     col("crs").getItem(2).substr(0, 7).as("lon2"), col("crs").getItem(3).substr(0, 8).as("lat2"),
     col("crs").getItem(4).substr(0, 7).as("lon3"), col("crs").getItem(5).substr(0, 8).as("lat3"),
     col("crs").getItem(6).substr(0, 7).as("lon4"), col("crs").getItem(7).substr(0, 8).as("lat4"),
     col("crs").getItem(8).substr(0, 7).as("lon5"), col("crs").getItem(9).substr(0, 8).as("lat5")
   )*/
 /*val zip = udf((xs: Seq[Long], ys: Seq[Seq[Seq[Seq[Double]]]]) => xs.zip(ys))
   /* Извлечение координат сетки square_id*/
   val d =spark.read.json(InitTables.path_mi_grid).select("features.properties.cellID","features.geometry.coordinates" )
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
   )*/



 /*  Объединение зон: туристическая, жилая, рабочая с картой-сеткой */
 val zones_map = zones.join(coordinates, coordinates.col("cellID") === zones.col("square_id"))
   .select(col("square_id"), col("month"), col("activity"),
     col("lat1"), col("lon1"), col("lat2"), col("lon2"),
     col("lat3"), col("lon3"), col("lat4"), col("lon4"),
     col("lat5"), col("lon5"))
 /*.coalesce(1)
     .write
     .option("header", "false")
     .option("delimiter", "\t")
     .mode("overwrite")
     .csv("./output/zones_map")*/
   /* ВЫборка данных об обозначениях значений показателей датчиков */
 InitTables.initTablePollutionLegend(spark)
 val pl = spark.table("pollution_legend")
   .withColumnRenamed("sensor_id", "sensor_id_legend")
   .select("sensor_id_legend", "sensor_street_name", "sensor_lat", "sensor_long", "sensor_type", "uom")

 InitTables.initTablePollutionMI(spark)
 val pmi = spark.table("pollution_mi")
 /* Соединение легенды по загрязнениям со значениями загрязнений*/
 val pmi_legend = pmi.join(pl, pmi("sensor_id") === pl("sensor_id_legend"), "inner")
   .select(col("sensor_id"), col("time_instant"), col("measurement"), col("sensor_street_name")
     .substr(0,8).as("sensor_street_name")
     , col("sensor_lat").substr(0, 8).as("sensor_lat"), col("sensor_long").substr(0, 8).as("sensor_long")
     , col("sensor_type"))
   .groupBy("sensor_id","sensor_lat","sensor_long","sensor_street_name","sensor_type")
   .sum("measurement")
   .withColumnRenamed("sum(measurement)","total_measurement")
   /* Проверка что датчик состоит в square_id*/
 pmi_legend.crossJoin(zones_map)
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
       && col("lon1") <= col("sensor_long"))))
   .coalesce(1)
   .write
   .option("header", "false")
   .option("delimiter", "\t")
   .mode("overwrite")
   .csv("./output/measurement")
    spark.stop()
  }
}
