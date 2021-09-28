package training

import breeze.numerics.round
import com.crealytics.spark.excel.ExcelDataFrameReader
import domain.{PollLegendCase, PollMiCase}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset.format
import org.apache.spark.sql.functions.{bround, col, date_format, explode, from_unixtime, length, ltrim, regexp_replace, rtrim, size, split, substring, to_date, to_utc_timestamp, trim, udf, when}
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, parseJson, parseJsonOpt}
import system.Parameters

import java.io.File
import scala.collection.mutable




object App extends App {
  val d = new File(Parameters.path_activity)
  val d1 = new File(Parameters.path_result_zone)

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark-sql-labs")
    .getOrCreate()


  if (!d.exists()) {


    Parameters.initTables(spark)

    val df = spark.table(Parameters.table_parquet)
      .sqlContext
      //Замена Null на 0.0
      .sql("Select distinct square_id, " +
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
        to_utc_timestamp(from_unixtime(col("time_interval") / 1000, "yyyy-MM-dd hh:mm:ss"), "Europe/Milan"))
      .withColumn("month", date_format(to_utc_timestamp(from_unixtime(col("time_interval") / 1000, "yyyy-MM-dd hh:mm:ss"), "Europe/Milan"), "LLLL"))
      .select("square_id", "time", "month", "activity")
      .coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(Parameters.path_activity)


  }
  else  {

    Parameters.initTableActivity(spark)
    import spark.implicits._

    val zones = spark.table("activity")
      .sqlContext
      .sql("SELECT square_id, month, time, " +
        "CASE " +
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
        "END square_ind " +
        "FROM activity")

    val d =spark.read.json(Parameters.path_mi_grid).select("features.properties.cellID","features.geometry.coordinates" )
    //val concatArray = udf((value:  Seq[Seq[Seq[Seq[Double]]]]) => {
    //value.map(_.map(_.map(_.mkString(" ", ",", " ")).mkString(" ", "", " ")).mkString(" ", "", " "))
    //})
    val toArray = udf[Array[String], String](_.split(","))

    val concatArray = udf((value:  Seq[Seq[Seq[Double]]]) => {
      value.flatten.flatten.mkString(",")
    })

    val zip = udf((xs: Seq[Long], ys:Seq[Seq[Seq[Seq[Double]]]]) => xs.zip(ys))


    val coordinates = d.select(col("coordinates"), col("cellId"))
      .withColumn("coord",explode(zip(col("cellId"),col("coordinates"))))
      .select(col("coord._1").as("cellId"),concatArray(col("coord._2")).as("coordinat"))
      .select(col("cellId"),toArray(col("coordinat")).as("crs"))
      .select(col("cellId"),
        col("crs").getItem(0).substr(0,7).as("lon1"),col("crs").getItem(1).substr(0,8).as("lat1"),
        col("crs").getItem(2).substr(0,7).as("lon2"),col("crs").getItem(3).substr(0,8).as("lat2"),
        col("crs").getItem(4).substr(0,7).as("lon3"),col("crs").getItem(5).substr(0,8).as("lat3"),
        col("crs").getItem(6).substr(0,7).as("lon4"),col("crs").getItem(7).substr(0,8).as("lat4"),
        col("crs").getItem(8).substr(0,7).as("lon5"),col("crs").getItem(9).substr(0,8).as("lat5")
      )
    val zones_map =  zones.join(coordinates,coordinates.col("cellID")===zones.col("square_id"))
        .select(col("square_id"),col("month"),col("time"),
          col("lat1"),col("lon1"),col("lat2"),col("lon2"),
          col("lat3"),col("lon3"),col("lat4"),col("lon4"),
          col("lat5"),col("lon5"))
    /*.coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv("./output/zones_map")*/

    Parameters.initTablePollutionLegend(spark)
    val pl = spark.table("pollution_legend")
      .withColumnRenamed("sensor_id","sensor_id_legend")
      .select("sensor_id_legend", "sensor_street_name", "sensor_lat","sensor_long","sensor_type","uom")

    Parameters.initTablePollutionMI(spark)
    val pmi = spark.table("pollution_mi")

    val pmi_legend = pmi.join(pl,pmi("sensor_id")===pl("sensor_id_legend"),"inner")
      .select(col("sensor_id"), col("time_instant"),col("measurement"),col("sensor_street_name")
        ,col("sensor_lat").substr(0,8).as("sensor_lat"),col("sensor_long").substr(0,8).as("sensor_long")
        ,col("sensor_type"))

      pmi_legend.crossJoin(coordinates)
        .filter(
          (col("lat1")===col("sensor_lat")) || (col("lon1")===col("sensor_long"))
            ||
            (col("lat2")===col("sensor_lat")) || (col("lon2")===col("sensor_long"))
            ||
            (col("lat3")===col("sensor_lat")) || (col("lon3")===col("sensor_long"))
            ||
            (col("lat4")===col("sensor_lat")) || (col("lon4")===col("sensor_long"))
            ||
            (col("lat5")===col("sensor_lat")) || (col("lon5")===col("sensor_long"))).show()

      /*.coalesce(1)
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(Parameters.path_result_zone)*/

    /*Parameters.initTablePollutionLegend(spark)

    val pl = spark.table("pollution_legend")
      .withColumnRenamed("sensor_id","sensor_id_legend")
      .select("sensor_id_legend", "sensor_street_name", "sensor_lat","sensor_long","sensor_type","uom")

    Parameters.initTablePollutionMI(spark)
    val pmi = spark.table("pollution_mi")

    val pmi_legend = pmi.join(pl,pmi("sensor_id")===pl("sensor_id_legend"),"inner")
      .select(col("sensor_id"), col("time_instant"),col("measurement"),col("sensor_street_name")
        ,col("sensor_lat"),col("sensor_long"),col("sensor_type"))*/

  }
  /*else {
    import spark.implicits._
    Parameters.initTablePollutionLegend(spark)
    val pl = spark.table("pollution_legend")
      .withColumnRenamed("sensor_id","sensor_id_legend")
      .select("sensor_id_legend", "sensor_street_name", "sensor_lat","sensor_long","sensor_type","uom")

    Parameters.initTablePollutionMI(spark)
    val pmi = spark.table("pollution_mi")

    val pmi_legend = pmi.join(pl,pmi("sensor_id")===pl("sensor_id_legend"),"inner")
      .select(col("sensor_id"), col("time_instant"),col("measurement"),col("sensor_street_name")
        ,col("sensor_lat"),col("sensor_long"),col("sensor_type"))
    /*.coalesce(1)
     .write
     .option("header", "false")
      .mode("overwrite")
     .csv(Parameters.path_result_legend)*/
    val zip = udf((xs: Seq[Long], ys:Seq[Seq[Seq[Seq[Double]]]]) => xs.zip(ys))

    val d =spark.read.json(Parameters.path_mi_grid).select("features.properties.cellID","features.geometry.coordinates" )
    //val concatArray = udf((value:  Seq[Seq[Seq[Seq[Double]]]]) => {
    //value.map(_.map(_.map(_.mkString(" ", ",", " ")).mkString(" ", "", " ")).mkString(" ", "", " "))
    //})
    val toArray = udf[Array[String], String](_.split(","))

    val concatArray = udf((value:  Seq[Seq[Seq[Double]]]) => {
      value.flatten.flatten.mkString(",")
    })


    val coordinates = d.select(col("coordinates"), col("cellId"))
      //.withColumn("vars",split(concatArray(col("coordinates")),",").cast("array<String>"))
      .withColumn("coord",explode(zip(col("cellId"),col("coordinates"))))
      .select(col("coord._1").as("cellId"),concatArray(col("coord._2")).as("coordinat"))
      .select(col("cellId"),toArray(col("coordinat")).as("crs"))
      .select(col("cellId"),
         col("crs").getItem(0).substr(0,8).as("lat1"),col("crs").getItem(1).substr(0,9).as("lon1"),
        col("crs").getItem(2).substr(0,8).as("lat2"),col("crs").getItem(3).substr(0,9).as("lon2"),
        col("crs").getItem(4).substr(0,8).as("lat3"),col("crs").getItem(5).substr(0,9).as("lon3"),
        col("crs").getItem(6).substr(0,8).as("lat4"),col("crs").getItem(7).substr(0,9).as("lon4"),
        col("crs").getItem(8).substr(0,8).as("lat5"),col("crs").getItem(9).substr(0,9).as("lon5")
      )

    coordinates.crossJoin(pmi_legend)
    .filter(
      (col("lat1")===col("sensor_lat")) && (col("lon1")===col("sensor_long"))
        ||
        (col("lat2")===col("sensor_lat")) && (col("lon2")===col("sensor_long"))
        ||
        (col("lat3")===col("sensor_lat")) && (col("lon3")===col("sensor_long"))
        ||
        (col("lat4")===col("sensor_lat")) && (col("lon4")===col("sensor_long"))
        ||
        (col("lat5")===col("sensor_lat")) && (col("lon5")===col("sensor_long"))
    ).show()
      /*.coalesce(1)
      .write
      .option("header", "false")
      .option("multiline","true")
      .mode("overwrite")
      .csv("./output/coordinates")*/






  }*/
  spark.stop()

}
