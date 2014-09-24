package sampleclean.clean.deduplication

import scala.collection.immutable.Map
import scala.reflect.ClassTag
import scala.collection.Seq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._


/**
  * This class contains functions to perform a similarity join between two data sets.
  * It uses a Prefix Filtering algorithm to reduce the number of calculations.
  * The algorithm assigns global weights to tokens so that common words are not influential when
  * determining similarity. It assigns IDF weights by default.
  */
trait WeightedPrefixFiltering extends Serializable {


 /**
   * Returns true if two token lists are similar; otherwise, returns false.
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  def isSimilar (tokens1: Seq[String], tokens2: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Boolean


 /**
   * Computes the sum of individual token weights over a token list.
   * If a token is not found on the given map, it assumes the token has a weight of 0.
   * @param tokens token list to be weighted
   * @param tokenWeights token-to-weight map
   */
  def sumWeight (tokens: Seq[String], tokenWeights: collection.Map[String, Double]): Double = {
      tokens.foldLeft(0.0) ((accum, token) => accum + tokenWeights.getOrElse(token, 0.0))
  }

 /**
   * Computes the number of tokens that can be removed from the tokenSet as per Prefix Filtering algorithm.
   * @param sortedTokens  token list. Must be sorted as per tokens' corresponding weights.
   * @param modThreshold modified threshold that depends on selected similarity measure.
   */
  def getRemovedSize (sortedTokens: Seq[String], modThreshold: Double, tokenWeights: collection.Map[String, Double]): Int = {
    val removedSize = {
      sortedTokens.foldRight((0.0, 0)) {
        case (token, (accum, count)) => {
          // weight is 0 if token does not have an assigned weight
          val current = accum + tokenWeights.getOrElse(token, 0.0)

          if (current < modThreshold) (current, count + 1) else (current, count)
        }
      }._2
    }

    if (removedSize > sortedTokens.size)
      sortedTokens.size
    else if (removedSize < 0)
      0
    else
      removedSize
  }

 /**
   * Sorts a token list based on token's frequency
   * @param tokens  list to be sorted.
   * @param tokenRanks Key-Value map of tokens and global ranks in ascending order (i.e. token with smallest value is rarest)
   */
  private def sortTokenSet(tokens: Seq[String], tokenRanks: Broadcast[Map[String, Int]])
  : Seq[String] = {
    tokens.map(token => (token, tokenRanks.value.getOrElse(token, 0))).toSeq.sortBy(_._2).map(_._1)
  }

 /**
   * Counts the number of times that each token shows up in the data
   * @param data  RDD with tokenized records.
   */
  private def computeTokenCount(data: RDD[(Seq[String])]): collection.Map[String, Int] = {
    data.flatMap{
      case tokens =>
        for (x <- tokens.distinct)
        yield (x, 1)
    }.reduceByKeyLocally(_ + _)
  }

 /**
   * Joins two data sets using Prefix Filtering, a similarity measure, IDF weights and a broadcasting procedure.
   * Broadcast variables may be too heavy for the driver to accept,
   * so adjusting driver memory accordingly is recommended.
   * @param sc given spark context.
   * @param threshold specified threshold.
   * @param fullTable second data set (e.g. full table)
   * @param fullKey second blocking method that will be used to calculate similarities between records.
   * @param sampleTable first data set (e.g. sample table)
   * @param sampleKey first blocking method that will be used to calculate similarities between records.
   *
   */
   def broadcastJoin (@transient sc: SparkContext,
                      threshold: Double,
                      fullTable: SchemaRDD,
                      fullKey: BlockingKey,
                      sampleTable: SchemaRDD,
                      sampleKey: BlockingKey): RDD[(Row,Row)] = {

   //Add a record ID into sampleTable. Id is a unique id assigned to each row.
    val sampleTableWithId: RDD[(Long, (Seq[String], Row))] = sampleTable.zipWithUniqueId
      .map(x => (x._2, (sampleKey.tokenSet(x._1), x._1))).cache()
    sampleTableWithId.setName("sampleTableWithId")

   // Materialize tokenCountMap for computing tokenRankMap and tokenWeightMap
    val tokenCountMap = computeTokenCount(sampleTableWithId.map(_._2._1))

   // Set a global order to all tokens based on their frequencies
    val tokenRankMap: Map[String, Int] = tokenCountMap.toArray.sortBy(_._2).map(_._1).zipWithIndex.toMap

   // Build a token-to-IDF map
    val tableSize = sampleTable.count()
    val tokenWeightMap = tokenCountMap.map(x => (x._1, math.log10(tableSize.toDouble / x._2)))

   // Broadcast rank map to all nodes
    val broadcastRank = sc.broadcast(tokenRankMap)

    // Build an inverted index for the prefixes of sample data
    val invertedIndex: RDD[(String, Seq[Long])] = sampleTableWithId.flatMap {
      case (id, (tokens, value)) =>
        val sorted = sortTokenSet(tokens, broadcastRank) // To Juan: Why do you use broadcast here?
        for (x <- sorted)
          yield (x, id)
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))

    //Broadcast data to all nodes
    val broadcastIndex: Broadcast[collection.Map[String, Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (Seq[String], Row)]] = sc.broadcast(sampleTableWithId.collectAsMap())
    val broadcastWeights: Broadcast[collection.Map[String, Double]] =  sc.broadcast(tokenWeightMap)

    //Generate the candidates whose prefixes have overlap, and then verify their overlap similarity
    fullTable.flatMap({
      case (row1) =>
        val weightsValue = broadcastWeights.value
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value

        val key1 = fullKey.tokenSet(row1)
        val sorted: Seq[String] = sortTokenSet(key1, broadcastRank)
        val removedSize = getRemovedSize(sorted, threshold, weightsValue)
        val filtered = sorted.dropRight(removedSize)

        filtered.foldLeft(List[Long]()) {
          case (a, b) =>
              a ++ broadcastIndexValue.getOrElse(b, List())
        }.distinct.map {
          case id =>
            val (key2, row2) = broadcastDataValue(id)
            val similar: Boolean = isSimilar(key1, key2, threshold, weightsValue)
            (key2, row2, similar)
        }.withFilter(_._3).map {
          case (key2, row2, similar) => (row1, row2)
        }
    })
  }

  /**
   * Performs a self-join using Prefix Filtering, a similarity measure, IDF weights and a broadcasting procedure.
   * Broadcast variables may be too heavy for the driver to accept,
   * so adjusting driver memory accordingly is recommended.
   * @param sc given spark context.
   * @param threshold specified threshold.
   * @param fullTable second data set (e.g. full table)
   * @param fullKey second blocking method that will be used to calculate similarities between records.
   */
  def broadcastJoin (@transient sc: SparkContext,
                     threshold: Double,
                     fullTable: SchemaRDD,
                     fullKey: BlockingKey): RDD[(Row,Row)] = {

    // Add record ID into sampleData: RDD[(Id, (Seq[K], Value))]
    val fullTableId: RDD[(Long, (Seq[String], Row))] = fullTable.zipWithUniqueId()
      .map(x => (x._2, (fullKey.tokenSet(x._1), x._1))).cache()

    val tokenCountMap = computeTokenCount(fullTableId.map(_._2._1))

    // Set a global order to all tokens based on their frequencies
    val tokenRankMap: Map[String, Int] = tokenCountMap.toArray.sortBy(_._2).map(_._1).zipWithIndex.toMap
    val broadcastRank = sc.broadcast(tokenRankMap)

    // Build a token-to-IDF map
    val tableSize = fullTable.count()
    val tokenWeightMap = tokenCountMap.map(x => (x._1, math.log10(tableSize.toDouble / x._2)))

    // Build an inverted index for the prefixes of sample data
    val invertedIndex: RDD[(String, Seq[Long])] = fullTableId.flatMap {
      case (id, (tokens, value)) =>
        val sorted = sortTokenSet(tokens, broadcastRank)
        for (x <- sorted)
        yield (x, id)
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))

    //Broadcast data to all nodes
    val broadcastIndex: Broadcast[collection.Map[String, Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (Seq[String], Row)]] = sc.broadcast(fullTableId.collectAsMap())
    val broadcastWeights: Broadcast[collection.Map[String, Double]] =  sc.broadcast(tokenWeightMap)

    //Generate the candidates whose prefixes have overlap, and then verify their overlap similarity
    fullTableId.flatMap({
      case (id1, (key1, row1)) =>
        val weightsValue = broadcastWeights.value
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value

        val sorted: Seq[String] = sortTokenSet(key1, broadcastRank)
        val removedSize = getRemovedSize(sorted, threshold, weightsValue)
        val filtered = sorted.dropRight(removedSize)

        filtered.foldLeft(List[Long]()) {
          case (a, b) =>
            a ++ broadcastIndexValue.getOrElse(b, List())
        }.distinct.map {
          case id2 =>
            // Avoid double checking
            if (id2 >= id1) (null, null, false)
            else {
              val (key2, row2) = broadcastDataValue(id1)
              val similar: Boolean = isSimilar(key1, key2, threshold, weightsValue)
              (key2, row2, similar)
            }
        }.withFilter(_._3).map {
          case (key2, row2, similar) => (row1, row2)
        }
    })
  }
}

