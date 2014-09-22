package sampleclean.clean.deduplication

import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import sampleclean.activeml.{DeduplicationGroupLabelingContext, DeduplicationPointLabelingContext, PointLabelingContext}
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
case class BlockingKey(cols: Seq[Int],
                       tokenizer: Tokenizer,
                       lowerCase: Boolean = true) {

  def tokenSet(row: Row): Seq[String] = {
    cols.flatMap{x =>
      var value = row.getString(x)
      if (lowerCase)
        value = value.toLowerCase()

      tokenizer.tokenSet(value)
    }
  }
}

case class SimilarityParameters(simFunc: String = "WJaccard",
                             threshold: Double = 0.5,
                             tokenizer: Tokenizer = WordTokenizer(),
                             lowerCase: Boolean = true)

/**
 * This class builds a blocking strategy for two data sets.
 * Blocking keys are created for each record and then compared against
 * the opposite data set in order to perform a similarity join.
 * param simFunc similarity algorithm to be used for blocking key comparison.
 * param threshold specified threshold.
 * param genKeySmallTable blocking method for the smaller data set.
 * param genKeyLargeTable blocking method for the larger data set.
 */
case class BlockingStrategy(blockedColNames: List[String]){

  var similarityParameters = SimilarityParameters()


  def setSimilarityParameters(similarityParameters: SimilarityParameters): BlockingStrategy = {
    this.similarityParameters = similarityParameters
    return this
  }

  def getSimilarityParameters(): SimilarityParameters = {
    return this.similarityParameters
  }

  def setThreshold(threshold: Double): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(similarityParameters.simFunc, threshold, similarityParameters.tokenizer, similarityParameters.lowerCase)
    return this
  }

  def getThreshold(): Double = {
    return similarityParameters.threshold
  }

  def setSimFunc(simFunc: String): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(simFunc, similarityParameters.threshold, similarityParameters.tokenizer, similarityParameters.lowerCase)
    return this
  }

  def getSimFunc(): String = {
    return similarityParameters.simFunc
  }
  def setTokenizer(tokenizer: Tokenizer): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(similarityParameters.simFunc, similarityParameters.threshold, tokenizer, similarityParameters.lowerCase)
    return this
  }
  def getTokenizer(): Tokenizer = {
    return similarityParameters.tokenizer
  }

  def setLowerCase(lowerCase: Boolean): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(similarityParameters.simFunc, similarityParameters.threshold, similarityParameters.tokenizer, lowerCase)
    return this
  }

  def getLowerCase(): Boolean = {
    return similarityParameters.lowerCase
  }

  /**
   * Runs a blocking algorithm on two data sets. Uses Spark SQL.
   * @param sc spark context
   * @param smallTable smallest data set (e.g. a sample of the full data set).
   * @param largeTable largest data set (e.g. full data set).
   */
  def blocking(@transient sc: SparkContext,
               largeTable: SchemaRDD,
               largeTableColMapper: List[String] => List[Int],
               smallTable: SchemaRDD,
               smallTableColMapper: List[String] => List[Int]
               ): RDD[(Row, Row)] = {

    val genKeyLargeTable = BlockingKey(largeTableColMapper(blockedColNames), similarityParameters.tokenizer)
    val genKeySmallTable = BlockingKey(smallTableColMapper(blockedColNames), similarityParameters.tokenizer)

    val simFunc = similarityParameters.simFunc
    val threshold = similarityParameters.threshold
    simFunc match {
      case "Jaccard" =>
        new JaccardJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "Overlap" =>
        new OverlapJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "Dice" =>
        new DiceJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "Cosine" =>
        new CosineJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "WJaccard" =>
        new WeightedJaccardJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "WOverlap" =>
        new WeightedOverlapJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "WDice" =>
        new WeightedDiceJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case "WCosine" =>
        new WeightedCosineJoin().broadcastJoin(sc, threshold, largeTable, genKeyLargeTable, smallTable, genKeySmallTable)
      case _ => println("Cannot support "+simFunc); null
    }
  }

  /**
   * Runs a blocking algorithm on two data sets. Uses Spark SQL.
   * @param sc spark context
   * @param table smallest data set (e.g. a sample of the full data set).
   * @param colMapper largest data set (e.g. full data set).
   */
  def blocking(@transient sc: SparkContext,
               table: SchemaRDD,
               colMapper: List[String] => List[Int]): RDD[(Row, Row)] = {

    val genKey = BlockingKey(colMapper(blockedColNames), similarityParameters.tokenizer)


    val simFunc = similarityParameters.simFunc
    val threshold = similarityParameters.threshold
    println("simFunc = " + simFunc + " threshold = " + threshold.toString)
    simFunc match {
      case "Jaccard" =>
        new JaccardJoin().broadcastJoin(sc, threshold, table, genKey)
      case "Overlap" =>
        new OverlapJoin().broadcastJoin(sc, threshold, table, genKey)
      case "Dice" =>
        new DiceJoin().broadcastJoin(sc, threshold, table, genKey)
      case "Cosine" =>
        new CosineJoin().broadcastJoin(sc, threshold, table, genKey)
      case "WJaccard" =>
        new WeightedJaccardJoin().broadcastJoin(sc, threshold, table, genKey)
      case "WOverlap" =>
        new WeightedOverlapJoin().broadcastJoin(sc, threshold, table, genKey)
      case "WDice" =>
        new WeightedDiceJoin().broadcastJoin(sc, threshold, table, genKey)
      case "WCosine" =>
        new WeightedCosineJoin().broadcastJoin(sc, threshold, table, genKey)
      case _ => println("Cannot support "+simFunc); null
    }
  }
}

