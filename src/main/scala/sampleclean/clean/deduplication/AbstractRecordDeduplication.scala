package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}


abstract class AbstractRecordDeduplication(params:AlgorithmParameters,
                                  scc: SampleCleanContext) extends
AbstractDeduplication(params, scc)  {


  def onReceiveCandidatePairs(candidatePairs: RDD[(Row, Row)],
                              sampleTableName:String):Unit = {
    val dupCounts = candidatePairs.map{case (fullRow, sampleRow) =>
      (scc.getColAsString(sampleRow, sampleTableName, "hash"),1)} // SHOULD unify hash and idCol
      .reduceByKey(_ + _)
      .map(x => (x._1,x._2+1)) // Add back the pairs that are removed above

    println("[SampleClean] Updating Sample Using Predicted Counts")
    scc.updateTableDuplicateCounts(sampleTableName, dupCounts)
  }

}
