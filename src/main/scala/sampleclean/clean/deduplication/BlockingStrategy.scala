package sampleclean.clean.deduplication

import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import sampleclean.activeml.{DeduplicationGroupLabelingContext, DeduplicationPointLabelingContext, PointLabelingContext}
import sampleclean.util.QueryBuilder._
import scala.collection.Seq
import scala.List
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
import sampleclean.api.SampleCleanContext
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
case class BlockingKey(cols: Seq[String],
                       tokenizer: Tokenizer,
                       getCol: (Row, String) => String,
                       lowerCase: Boolean = true) {

  def tokenSet(row: Row): Seq[String] = {
    cols.flatMap{x =>
      var value = getCol(row,x)
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
 * @param genKeySmallTable blocking method for the smaller data set.
 * @param genKeyLargeTable blocking method for the larger data set.
 * @param joinType type of join: "sampleJoin", "selfJoin" or standard similarity join ("stJoin").
 */
case class BlockingStrategy(simFunc: String,
                       threshold: Double,
                       joinType: String,
                       genKeyLargeTable: BlockingKey,
                       genKeySmallTable: BlockingKey = null
                       ){

  val gKLarge = genKeyLargeTable
  val gKSmall = Option(genKeySmallTable).getOrElse(genKeyLargeTable)
  
  def getJoinType = joinType

  /**
   * Runs a blocking algorithm on two data sets. Uses Spark SQL.
   * @param sc spark context
   * @param smallTable smallest data set (e.g. a sample of the full data set).
   * @param largeTable largest data set (e.g. full data set).
   */
  def blocking(@transient sc: SparkContext,
               largeTable: SchemaRDD,
               smallTable: SchemaRDD
               ): RDD[(Row, Row)] = {

    (simFunc, joinType, Option(smallTable).isEmpty) match {
      case ("Jaccard", "sampleJoin", false) =>
        new JaccardJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("Overlap", "sampleJoin", false) =>
        new OverlapJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("Dice", "sampleJoin", false) =>
        new DiceJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("Cosine", "sampleJoin", false) =>
        new CosineJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("WJaccard", "sampleJoin", false) =>
        new WeightedJaccardJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("WOverlap", "sampleJoin", false) =>
        new WeightedOverlapJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("WDice", "sampleJoin", false) =>
        new WeightedDiceJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      case ("WCosine", "sampleJoin", false) =>
        new WeightedCosineJoin().broadcastJoin(sc, smallTable, largeTable, gKSmall, gKLarge, threshold)
      // cases for self-joins
      case ("Jaccard", "selfJoin", true) =>
        new JaccardJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("Overlap", "selfJoin", true) =>
        new OverlapJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("Dice", "selfJoin", true) =>
        new DiceJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("Cosine", "selfJoin", true) =>
        new CosineJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("WJaccard", "selfJoin", true) =>
        new WeightedJaccardJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("WOverlap", "selfJoin", true) =>
        new WeightedOverlapJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("WDice", "selfJoin", true) =>
        new WeightedDiceJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case ("WCosine", "selfJoin", true) =>
        new WeightedCosineJoin().broadcastSelfJoin(sc, largeTable, gKLarge, threshold)
      case _ => null
    }
  }
}

