package sampleclean.clean.deduplication.matcher

import org.apache.spark.sql.Row
import scala.util.Random

/**
 * Abstract class that defines a rule for choosing a matcher
 * that will be applied on a pair of rows.
 */
abstract class MatcherPredicate (val matchers: List[Matcher]) extends Serializable {
  def chooseMatcher(row1: Row, row2:Row): (Int,Matcher)
}

object MatcherPredicate {

  /**
   * This class chooses matchers based on their probabilities
   */
  class ProbabilityPredicate(var input: List[(Matcher,Double)]) extends MatcherPredicate(input.map(_._1)) {

    def this (matcher_1: Matcher, matcher_2: Matcher){
      this (List((matcher_1,0.5),(matcher_2,0.5)))
    }

    validate()
    var buckets: List[(Matcher,(Double,Double))] = List()

    var start = 0.0

    for (pair <- input){
      buckets = buckets :+ (pair._1, (start, start + pair._2))
      start += pair._2
    }

    def validate() = {
      if (input.map(_._2).foldLeft(0.0)(_ + _) != 1.0 | !input.forall(_._2 >= 0.0))
        throw new RuntimeException("Input must be a list of probabilities")
    }

    def chooseMatcher(row1: Row, row2: Row): (Int,Matcher) = {
      val i = new Random().nextDouble()
      var result = (input.size - 1, input.last._1)

      var count = 0
      for (matcher <- buckets) {
        if (i >= matcher._2._1 && i < matcher._2._2)
          result = (count, matcher._1)

        count += 1
      }
      result

    }


  }

}
