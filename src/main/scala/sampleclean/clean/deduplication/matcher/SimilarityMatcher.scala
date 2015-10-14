package sampleclean.clean.deduplication.matcher

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.api.SampleCleanContext
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer


class SimilarityMatcher(scc: SampleCleanContext,
                        sampleName:String,
                        featurizer: AnnotatedSimilarityFeaturizer) extends Matcher(scc,sampleName){

  val asynchronous = false

  // override if needed
  def matchRule(row1: Row,row2: Row): Boolean = {
    featurizer.featurize(Set(row1, row2))._2(0) == 1.0
  }


  def matchPairs(candidatePairs: => RDD[Set[Row]]): RDD[(Row,Row)] = {
    matchPairs(candidatePairs.flatMap(selfCartesianProduct))
  }

  def matchPairs(input:RDD[(Row,Row)]):RDD[(Row,Row)] = {
    input.foreach(x => {
    })
    val sim = input.filter(x => matchRule(x._1,x._2))
    sim
  }

  override def updateContext(newContext:List[String]) ={
    context = newContext
    featurizer.setContext(newContext)
  }



}
