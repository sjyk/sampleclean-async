package sampleclean.util

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/** Types sometimes have to be sanitized before data cleaning
* this class handles type conversions and HIVEQL type casting.
*/
@serializable
object TypeUtils {

  /** SparkSQL cols are of the format Any,
  *this class casts the attr to a Double.
  */
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

  /** The COALESCE HiveQL command returns the 
  *first not null values. This allows us to
  * default improperly formated numbers to a 
  * value.
  */
  def typeSafeHQL(attr:String, defaultVal:Int=0):String = {
    return "COALESCE(" +attr+"-0," + defaultVal+")"
  }

}