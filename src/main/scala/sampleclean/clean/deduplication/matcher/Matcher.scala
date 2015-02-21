package sampleclean.clean.deduplication.matcher

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

abstract class Matcher(scc: SampleCleanContext,
              		   sampleTableName:String) extends Serializable {

  def matchPairs(input: => RDD[Set[Row]]):RDD[(Row,Row)]

  def matchPairs(input:RDD[(Row,Row)]):RDD[(Row,Row)]


  def selfCartesianProduct(rowSet: Set[Row]):List[(Row,Row)] = {

    var crossProduct:List[(Row,Row)] = List()
    for (r <- rowSet)
      for (s <- rowSet)
        if (r != s)
          crossProduct = (r,s) :: crossProduct
    return crossProduct

  }

}


