package domain

import domain.Parquet.Value
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.sql.Date

object Activity extends Enumeration {

  val square_id, time, month, avg_activity,
  activity = Value


  val structType = StructType(
    Seq(
      StructField(square_id.toString, IntegerType),
      StructField(time.toString, StringType),
      StructField(month.toString, StringType),
      StructField(avg_activity.toString, DoubleType),
      StructField(activity.toString, DoubleType)
    )
  )

}

case class ActivityCase(

                        square_id: Int,
                        time: String,
                        avg_activity: Double,
                        activity: Double)
