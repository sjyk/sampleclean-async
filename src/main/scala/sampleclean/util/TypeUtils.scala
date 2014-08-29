package sampleclean.util

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row

@serializable
object TypeUtils {

  def rowToNumber(row:Row, col:Int): Double = {

  	  try{
  		if (row(col).isInstanceOf[Int])
  			return row(col).asInstanceOf[Int].toDouble
  		else
  			return row(0).asInstanceOf[Double]
  		}
  	   catch{
     		case e: Exception => Double.NaN
   		}
  
  }


}