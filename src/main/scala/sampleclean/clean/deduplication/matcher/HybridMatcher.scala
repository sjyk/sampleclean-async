package sampleclean.clean.deduplication.matcher

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.api.SampleCleanContext

/**
 * A matcher that matches pairs or sets of rows based on two other matchers
 * and some rule.
 * @param scc Sample Clean context
 * @param sampleName
 * @param p contains matchers and rule that will decide which matcher to use on each pair of rows
 */
class HybridMatcher(scc: SampleCleanContext,
                    sampleName:String,
                    p:MatcherPredicate) extends Matcher(scc,sampleName) {


  val asynchronous: Boolean = !p.matchers.forall(!_.asynchronous)


  def matchPairs(input: => RDD[Set[Row]]):RDD[(Row,Row)] = {
    matchPairs(input.flatMap(selfCartesianProduct))
  }

  def matchPairs(input:RDD[(Row,Row)]):RDD[(Row,Row)] = {
    p.matchers.foreach(m => if (m.asynchronous) m.onReceiveNewMatches = onReceiveNewMatches)

    p.matchers.foreach(m => println(" m on R: " + m.onReceiveNewMatches))
    val chosenMatchers = input.map(pair => (pair,p.chooseMatcher(pair._1,pair._2))).cache()

    var result: RDD[(Row,Row)] = scc.getSparkContext().emptyRDD[(Row,Row)]
    p.matchers.indices.foreach ({i =>
      val m = p.matchers(i)
      val matched = m.matchPairs(chosenMatchers.filter(_._2._1 == i).map(_._1))

      result = result.union(matched)
    })

    val r = result.distinct()
    chosenMatchers.unpersist()
    r
  }

  override def updateContext(newContext:List[String]) ={
    context = newContext
    p.matchers.foreach(_.updateContext(newContext))
  }

}
