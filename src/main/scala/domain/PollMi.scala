package domain

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object PollMi extends Enumeration {

  val sensor_id, time_instant, measurement = Value

  val structType = StructType(
    Seq(
      StructField(sensor_id.toString, IntegerType),
      StructField(time_instant.toString, StringType),
      StructField(measurement.toString, DoubleType)
    )
  )
}

case class PollMiCase( sensor_id:Int,
                       time_instant:String,
                       measurement: Double)

