package sampleclean.clean.deduplication

import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import scala.collection.Seq
import scala.List
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
//import org.amplab.sampleclean.cleaning.{WeightedCosineJoin, WeightedDiceJoin, WeightedOverlapJoin, WeightedJaccardJoin}


/**
 * This is a tokenizer super-class.
 */
trait Tokenizer extends Serializable {
  def tokenSet(text: String): List[String]
}

// Use | to separate multiple delimiters
/**
 * This class tokenizes a string based on user-specified delimiters.
 * @param delimiters string of delimiters to be used for splitting. Accepts regex expressions.
 */
case class DelimiterTokenizer(delimiters: String = ".,?!\t ") extends Tokenizer {

  def tokenSet(str: String) = {
    val st = new StringTokenizer(str, delimiters)
    val tokens = new ArrayBuffer[String]
    while (st.hasMoreTokens()) {
      tokens += st.nextToken()
    }
    tokens.toList
  }
}

/**
 * This class tokenizes a string based on words.
 */
case class WordTokenizer() extends Tokenizer {
  def tokenSet(str: String) = str.split("\\W+").toList.filter(_!="")
}

/**
 * This class tokenizes a string based on white spaces.
 */
case class WhiteSpaceTokenizer() extends Tokenizer {
  def tokenSet(str: String) = str.split("\\s+").toList.filter(_!="")
}

/**
 * This class tokenizes a string based on grams.
 */
case class GramTokenizer(gramSize: Int) extends Tokenizer {
  def tokenSet(str: String) =  str.sliding(gramSize).toList
}

/**
 * This class builds a method to tokenize rows of data.
 * @param cols columns to be tokenized.
 * @param tokenizer chosen tokenizer to be used.
 * @param lowerCase if true, convert all characters to lower case.
 */
case class BlockingKey(cols: Seq[Int], tokenizer: Tokenizer, lowerCase: Boolean = true) {

  def tokenSet(row: Row): Seq[String] = {
    cols.flatMap{ case x =>
      var value = row(x).toString()
      if (lowerCase)
        value = value.toLowerCase()
      tokenizer.tokenSet(value)
    }
  }
}

/**
 * This class builds a blocking strategy for two data sets.
 * Blocking keys are created for each record and then compared against
 * the opposite data set in order to perform a similarity join.
 * @param simFunc similarity algorithm to be used for blocking key comparison.
 * @param threshold specified threshold.
 * @param sampleKey blocking method for the first data set.
 * @param fullKey blocking method for the second data set.
 */
case class BlockingStrategy(simFunc: String,
                       threshold: Double,
                       sampleKey: BlockingKey,
                       fullKey: BlockingKey) {

  /**
   * Runs a blocking algorithm on two data sets. Uses Spark SQL.
   * @param sc spark context
   * @param sampleTable first data set; smallest (e.g. a sample of the full data set).
   * @param fullTable second data set; largest (e.g. full data set).
   */
  def blocking(@transient sc: SparkContext,
               sampleTable: SchemaRDD,
               fullTable: SchemaRDD): RDD[(Row, Row)] = {

    simFunc match {
      case "Jaccard" =>
        new JaccardJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "Overlap" =>
        new OverlapJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "Dice" =>
        new DiceJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "Cosine" =>
        new CosineJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "WJaccard" =>
        new WeightedJaccardJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "WOverlap" =>
        new WeightedOverlapJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "WDice" =>
        new WeightedDiceJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
      case "WCosine" =>
        new WeightedCosineJoin().broadcastJoin(sc, sampleTable, fullTable, sampleKey, fullKey, threshold)
    }
  }
}


 /* def blocking[K: ClassTag, V:ClassTag](SampleCleanContext scc,
                                     sampleData: SchemaRDD,
                                     fullData: SchemaRDD,
                                     simfunc: String)
  : RDD[((Long,Long),(Seq[K], Seq[K]), (V, V))] = {


    simfunc match {
      case "Jaccard" =>
        if (broadcast)
          new JaccardJoin().broadcastJoin(sc, sampleData, fullData, threshold)
        else
          new JaccardJoin().broadcastJoin(sc, sampleData, fullData, threshold)
      case "Overlap" =>
        if (broadcast)
          new OverlapJoin().broadcastJoin(sc, sampleData, fullData, threshold)
        else
          new OverlapJoin().broadcastJoin(sc, sampleData, fullData, threshold)
      case "Dice" =>
        if (broadcast)
          new DiceJoin().broadcastJoin(sc, sampleData, fullData, threshold)
        else
          new DiceJoin().broadcastJoin(sc, sampleData, fullData, threshold)
      case "Cosine" =>
        if (broadcast)
          new CosineJoin().broadcastJoin(sc, sampleData, fullData, threshold)
        else
          new CosineJoin().broadcastJoin(sc, sampleData, fullData, threshold)

*/

