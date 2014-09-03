package sampleclean.util

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

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

  def typeSafeHQL(attr:String):String = {
    return "COALESCE(" +attr+"-0,0)"
  }

  def typeSafeHQL(attr:String, defaultVal:Int):String = {
    return "COALESCE(" +attr+"-0," + defaultVal+")"
  }

}