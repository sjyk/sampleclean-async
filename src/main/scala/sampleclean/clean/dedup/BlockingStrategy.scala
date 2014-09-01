package sampleclean.clean.dedup



import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.collection.Seq
import scala.List
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
import org.amplab.sampleclean.cleaning.{WeightedCosineJoin, WeightedDiceJoin, WeightedOverlapJoin, WeightedJaccardJoin}


/**
 * Created by jnwang on 8/30/14.
 */

trait Tokenizer extends Serializable {
  def tokenSet(text: String): List[String]
}

// Use | to separate multiple delimiters
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


case class WordTokenizer() extends Tokenizer {
  def tokenSet(str: String) = str.split("\\W+").toList.filter(_!="")
}

case class WhiteSpaceTokenizer() extends Tokenizer {
  def tokenSet(str: String) = str.split("\\s+").toList.filter(_!="")
}

case class GramTokenizer(gramSize: Int) extends Tokenizer {
  def tokenSet(str: String) =  str.sliding(gramSize).toList
}

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


case class BlockingStrategy(simFunc: String,
                       threshold: Double,
                       sampleKey: BlockingKey,
                       fullKey: BlockingKey) {

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

