package sampleclean.clean.deduplication

import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import scala.collection.Seq
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
//import org.amplab.sampleclean.cleaning.{WeightedCosineJoin, WeightedDiceJoin, WeightedOverlapJoin, WeightedJaccardJoin}

/**
 * This is a tokenizer super-class.
 */
trait Tokenizer extends Serializable {
  def tokenSet(text: String): List[String]
}

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
 * @param gramSize size of gram.
 */
case class GramTokenizer(gramSize: Int) extends Tokenizer {
  def tokenSet(str: String) =  str.sliding(gramSize).toList
}

/**
 * This class builds a method to tokenize rows of data.
 * @param cols indices of columns to be tokenized.
 * @param tokenizer chosen tokenizer to be used.
 * @param lowerCase if true, convert all characters to lower case.
 */
case class BlockingKey(cols: Seq[Int],
                       tokenizer: Tokenizer,
                       lowerCase: Boolean = true) {

  /**
   * Returns string values from a row that are located on the chosen set of columns.
   * @param row row to be parsed.
   */
  def tokenSet(row: Row): Seq[String] = {
    cols.flatMap{x =>
      var value = row.getString(x)
      if (lowerCase)
        value = value.toLowerCase()

      tokenizer.tokenSet(value)
    }
  }

  def concat(row: Row): String = {
    cols.flatMap{x =>
      var value = row.getString(x)
      if (lowerCase)
        value = value.toLowerCase()

      tokenizer.tokenSet(value)
    }.mkString(" ")
  }
}


/**
 * This class defines default similarity parameters for the blocking strategy
 * @param simFunc chosen similarity function. Default: "WJaccard"
 *                (weighted Jaccard similarity)
 * @param threshold threshold that relates to similarity function. Default: 0.5.
 * @param tokenizer tokenizer function. Default: Word Tokenizer.
 * @param lowerCase if true, convert all strings to lower case. Default: true.
 */
case class SimilarityParameters(simFunc: String = "WJaccard",
                             threshold: Double = 0.5,
                             tokenizer: Tokenizer = WordTokenizer(),
                             lowerCase: Boolean = true)

/**
 * This class builds a blocking strategy for a data set.
 * Blocking keys are created according to specified column names.
 * @param blockedColNames column names to be used for blocking.
 */
case class BlockingStrategy(blockedColNames: List[String]){

  var similarityParameters = SimilarityParameters()

  /**
   * Used to set new similarity parameters.
   * @param similarityParameters new parameters.
   */
  def setSimilarityParameters(similarityParameters: SimilarityParameters): BlockingStrategy = {
    this.similarityParameters = similarityParameters
    return this
  }

  /** Returns current parameters.*/
  def getSimilarityParameters(): SimilarityParameters = {
    return this.similarityParameters
  }

  /**
   * Used to set a new similarity threshold which depends on the current similarity metric.
   * @param threshold new threshold.
   */
  def setThreshold(threshold: Double): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(similarityParameters.simFunc, threshold, similarityParameters.tokenizer, similarityParameters.lowerCase)
    return this
  }

  /** Returns current threshold.*/
  def getThreshold(): Double = {
    return similarityParameters.threshold
  }

  /**
   * Used to set a new similarity function.
   * @param simFunc name of new function which must be one of the following:
   *                Jaccard, Overlap, Dice, Cosine or a weighted version: WJaccard, WOverlap, WDice or WCosine.
   */
  def setSimFunc(simFunc: String): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(simFunc, similarityParameters.threshold, similarityParameters.tokenizer, similarityParameters.lowerCase)
    return this
  }

  /** Returns current similarity function.*/
  def getSimFunc(): String = {
    return similarityParameters.simFunc
  }

  /**
   * Used to set a new tokenizer.
   * @param tokenizer new tokenizer.
   */
  def setTokenizer(tokenizer: Tokenizer): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(similarityParameters.simFunc, similarityParameters.threshold, tokenizer, similarityParameters.lowerCase)
    return this
  }

  /** Returns current tokenizer.*/
  def getTokenizer(): Tokenizer = {
    return similarityParameters.tokenizer
  }

  /**
   * Used to set "lowerCase" parameter.
   * @param lowerCase if true, convert all strings to lower case.
   */
  def setLowerCase(lowerCase: Boolean): BlockingStrategy = {
    similarityParameters
      = SimilarityParameters(similarityParameters.simFunc, similarityParameters.threshold, similarityParameters.tokenizer, lowerCase)
    return this
  }

  /** Returns "lowerCase" parameter.*/
  def getLowerCase(): Boolean = {
    return similarityParameters.lowerCase
  }

  /**
   * Runs a blocking algorithm on two data sets. Uses Spark SQL.
   * @param sc spark context
   * @param largeTable largest data set (e.g. full data set).
   * @param largeTableColMapper function that converts a list of column names
   *                            in the large table into a list of those columns' indices.
   * @param smallTable smallest data set (e.g. a sample of the full data set).
   * @param smallTableColMapper function that converts a list of column names
   *                            in the small table into a list of those columns' indices.
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
   * Runs a blocking algorithm on one data set. Uses Spark SQL.
   * @param sc spark context
   * @param table data set.
   * @param colMapper function that converts a list of column names
   *                  into a list of those columns' indices.
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

