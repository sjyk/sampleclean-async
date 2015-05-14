package sampleclean.clean.deduplication.matcher

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * The AllMatcher class is used to match all similar candidates
 * in the input RDD.
 *
 */
class AllMatcher(scc: SampleCleanContext, 
				 sampleTableName: String) extends
				 Matcher(scc, sampleTableName) {

  val asynchronous = false

  def matchPairs(candidatePairs:RDD[(Row,Row)]): RDD[(Row,Row)] = {
      return candidatePairs
  	}

    def matchPairs(candidatePairs: => RDD[Set[Row]]): RDD[(Row,Row)] = {
      return candidatePairs.flatMap(selfCartesianProduct)
  	}


}
