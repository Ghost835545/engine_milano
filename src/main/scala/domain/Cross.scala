package domain

import domain.Activity.Value
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Cross extends Enumeration {




  val cellid, lat1, lon1, lat2, lon2, lat3, lon3, lat4, lon4, lat5, lon5
   = Value


  val structType = StructType(
    Seq(
      StructField(cellid.toString, StringType),
      StructField(lat1.toString, DoubleType),
      StructField(lon1.toString, DoubleType),
      StructField(lat2.toString, DoubleType),
      StructField(lon2.toString, DoubleType),
      StructField(lat3.toString, DoubleType),
      StructField(lon3.toString, DoubleType),
      StructField(lat4.toString, DoubleType),
      StructField(lon4.toString, DoubleType),
      StructField(lat5.toString, DoubleType),
      StructField(lon5.toString, DoubleType)
    )
  )

}


