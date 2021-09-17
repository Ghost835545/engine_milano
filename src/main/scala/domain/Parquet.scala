package domain

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

import java.sql.Date

object Parquet extends Enumeration {

  private val DELIMITER = "\\t"

  val square_id, time_interval, sms_in_activity,
  sms_out_activity, call_in_activity, call_out_activity, internet_traffic_activity, month = Value

  val structType = StructType(
    Seq(
      StructField(square_id.toString, IntegerType),
      StructField(time_interval.toString, DateType),
      StructField(sms_in_activity.toString, DoubleType),
      StructField(sms_out_activity.toString, DoubleType),
      StructField(call_in_activity.toString, DoubleType),
      StructField(call_out_activity.toString, DoubleType),
      StructField(internet_traffic_activity.toString, DoubleType),
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
