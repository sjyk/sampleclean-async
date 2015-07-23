package sampleclean.clean.deduplication.matcher

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.api.SampleCleanContext
import sampleclean.clean.deduplication.ActiveLearningStrategy
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.SimilarityFeaturizer
import sampleclean.clean.featurize.Tokenizer.WordTokenizer

/**
 * A matcher that matches pairs or sets of rows based on two other matchers
 * and some rule.
 * @param scc Sample Clean context
 * @param sampleName
 * @param p contains matcher components and rule that will decide which matcher to use on each pair of rows
 */
class HybridMatcher(scc: SampleCleanContext,
                    sampleName:String,
                    val p:MatcherPredicate) extends Matcher(scc,sampleName) {


  val asynchronous: Boolean = !p.matchers.forall(!_.asynchronous)
  val matchers: List[Matcher] = p.matchers
  validate()

  def validate() = {
    if (matchers.count(_.asynchronous) > 1)
      throw new RuntimeException("Only one asynchronous matcher allowed")
  }

  def matchPairs(input: => RDD[Set[Row]]):RDD[(Row,Row)] = {
    matchPairs(input.flatMap(selfCartesianProduct))
  }

  def matchPairs(input:RDD[(Row,Row)]):RDD[(Row,Row)] = {
    p.matchers.foreach(m => if (m.asynchronous) m.onReceiveNewMatches = onReceiveNewMatches)

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

object DefaultHybridMatcher {

  /**
   * Hybrid matcher adapted for Entity Resolution tasks.
   * It uses Jaccard similarity as a featurizer for
   * the automatic matcher and Levenshtein and JaroWinkler
   * features for the Active Learning matcher.
   *
   * @param scc SampleClean context
   * @param sampleName
   * @param attribute attribute to resolve
   * @param threshold threshold that will be used by
   *                       automatic matcher component
   * @param crowd_ratio ratio of candidate pairs that
   *                    will be matched with crowd matcher
   */
  def forEntityResolution(scc:SampleCleanContext,
                          sampleName:String,
                          attribute:String,
                          threshold:Double,
                          crowd_ratio:Double) = {
    // Auto matcher
    val featurizer = new WeightedJaccardSimilarity(List(attribute),
      scc.getTableContext(sampleName),
      WordTokenizer(),
      threshold)
    val autoMatcher = new SimilarityMatcher(scc, sampleName,featurizer)

    // AL matcher
    val baseFeaturizer = new SimilarityFeaturizer(List(attribute),
      scc.getTableContext(sampleName),
      List("Levenshtein", "JaroWinkler"))
    val alStrategy = new ActiveLearningStrategy(List(attribute), baseFeaturizer)
    val activeMatcher = new ActiveLearningMatcher(scc, sampleName, alStrategy)

    // Hybrid Matcher
    val p = new MatcherPredicate.ProbabilityPredicate(List((autoMatcher,1-crowd_ratio),(activeMatcher,crowd_ratio)))
    new HybridMatcher(scc,sampleName,p)
  }
}
