package sampleclean.clean.deduplication.matcher

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import sampleclean.api.SampleCleanContext

class CorleoneMatcher (scc: SampleCleanContext, sampleTableName:String) extends Matcher(scc, sampleTableName) {
  def matchPairs(candidatePairs: RDD[(Row,Row)]): RDD[(Row,Row)] = {
    // train a random forest on the candidate pairs w/ AL, maintaining labeled pairs
    // extract negative rules from the forest
    // select k=20 (from the paper) rules:
    // for each rule:
    //   compute cov(R, candidatePairs) as subset of candidatePairs that R votes 'no'
    //   estimate prec(R, candidatePairs) as |cov(R, cP) - positive labeled pairs| / cov(R, cP)
    // sort rules by prec(R, cP), tiebreaking with |cov(R, cP)|
    // take top k rules
    //
    // Corleone next step: Crowd Eval to reduce rules
    //
    // Corleone next step: greedy solution to pick final blocking rules
    //
    // Corleone next step: train a matcher on the tuples not ruled out by the blocking rules
  }

  def matchPairs(candidatePairs: => RDD[Set[Row]]): RDD[(Row,Row)] = {
    return matchPairs(candidatePairs.flatMap(selfCartesianProduct))
  }
}
