package domain
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object PollLegend extends Enumeration {

  val sensor_id, sensor_street_name, sensor_lat, sensor_long,
  sensor_type, call_in_activity, uom, time_instant_format = Value


  val structType = StructType(
    Seq(
      StructField(sensor_id.toString, IntegerType),
      StructField(sensor_street_name.toString, StringType),
      StructField(sensor_lat.toString, DoubleType),
      StructField(sensor_long.toString, DoubleType),
      StructField(sensor_type.toString, StringType),
      StructField(uom.toString, StringType),
      StructField(time_instant_format.toString, StringType)
    )
  )

}

case class PollLegendCase(

                        sensor_id:Int,
                        sensor_street_name:String,
                        sensor_lat: Double,
                        sensor_long: Double,
                        sensor_type: String,
                        uom: String,
                        time_instant_format:String)

