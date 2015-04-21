package sampleclean.clean.deduplication.matcher

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * A matcher provides methods to process two types of inputs
 * either a RDD[Set[Row]] or RDD[(Row,Row)].
 * Accordingly, we provide the "selfCartesianProduct" 
 * method to help with the conversion. There are some matchers
 * that are asynchronous, for these we require that the
 * onReceiveNewMatches is set. This is a function that 
 * takes some action based on new results.
 * 
 * @type {[type]}
 */
abstract class Matcher(scc: SampleCleanContext,
              		   sampleTableName:String) extends Serializable {

  var context:List[(String)] = scc.getTableContext(sampleTableName)

  def matchPairs(input: => RDD[Set[Row]]):RDD[(Row,Row)]

  def matchPairs(input:RDD[(Row,Row)]):RDD[(Row,Row)]

  val asynchronous:Boolean

  /**
   * For Set(a,b), a == b, it does not return (a,a) or (b,b)
   * and returns only (a,b) not (b,a)
   */
  def selfCartesianProduct(rowSet: Set[Row]):List[(Row,Row)] = {

    var crossProduct:List[(Row,Row)] = List()
    var tempSet:Set[Row] = rowSet
    for (r <- rowSet) {
      tempSet = tempSet.drop(1)
      for (s <- tempSet)
        crossProduct = crossProduct :+ (r, s)
    }
    return crossProduct

  }

  def updateContext(newContext:List[String]) ={
      context = newContext
  }

  var onReceiveNewMatches: RDD[(Row,Row)] => Unit = null

}


