package domain

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

import java.sql.Date

object Parquet extends Enumeration {

  private val DELIMITER = "\\t"

  val square_id, time_interval, sms_in_activity,
  sms_out_activity, call_in_activity, call_out_activity, internet_traffic_activity, month = Value

  val structType = StructType(
    Seq(
      StructField(square_id.toString, StringType),
      StructField(time_interval.toString, StringType),
      StructField(sms_in_activity.toString, StringType),
      StructField(sms_out_activity.toString, StringType),
      StructField(call_in_activity.toString, StringType),
      StructField(call_out_activity.toString, StringType),
      StructField(internet_traffic_activity.toString, StringType),
      StructField(month.toString, StringType)

    )
  )
}

case class CustomerCase(

                         square_id: Int,
                         time_interval: Date,
                         sms_in_activity: Double,
                         sms_out_activity: Double,
                         call_in_activity : Double,
                         call_out_activity: Double,
                         internet_traffic_activity : Double,
                         month : String)
