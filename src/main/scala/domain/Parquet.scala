package domain

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

import java.sql.Date

object Parquet extends Enumeration {

  val square_id, time_interval, country_code, sms_in_activity,
  sms_out_activity, call_in_activity, call_out_activity, internet_traffic_activity = Value


  val structType = StructType(
    Seq(
      StructField(square_id.toString, IntegerType),
      StructField(time_interval.toString, StringType),
      StructField(country_code.toString, StringType),
      StructField(sms_in_activity.toString, DoubleType),
      StructField(sms_out_activity.toString, DoubleType),
      StructField(call_in_activity.toString, DoubleType),
      StructField(call_out_activity.toString, DoubleType),
      StructField(internet_traffic_activity.toString, DoubleType)
    )
  )

}

case class ParquetCase(

                         square_id: Int,
                         time_interval: Date,
                         country_code: Int,
                         sms_in_activity: Double,
                         sms_out_activity: Double,
                         call_in_activity : Double,
                         call_out_activity: Double,
                         internet_traffic_activity : Double)
