package org.amplab.sampleclean.cleaning

import scala.collection.immutable.Map
import scala.reflect.ClassTag
import scala.collection.Seq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import sampleclean.clean.dedup.BlockingKey
import sampleclean.clean.dedup.BlockingKey

/**
 * Created by juanmanuelsanchez on 8/4/14.
 */


//  also supports regular Prefix Filtering
trait WeightedPrefixFiltering extends Serializable {


  // return true if tokenSet1 and tokenSet2 are similar; otherwise, return false
  def isSimilar (tokenSet1: Seq[String], tokenSet2: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Boolean

  // compute total weight in a set
  // weight is 0 if token does not have an assigned weight, weight is 1 if no token weights are provided
  def sumWeight (tokenSet: Seq[String], tokenWeights: collection.Map[String, Double]): Double = {
      tokenSet.foldLeft(0.0) ((accum, token) => accum + tokenWeights.getOrElse(token, 0.0))
  }


  // compute the number of tokens that can be removed from the tokenSet using weights
  def getRemovedSize (tokenSet: Seq[String], modThreshold: Double, tokenWeights: collection.Map[String, Double]): Int = {
    val removedSize = {
      tokenSet.foldRight((0.0, 0)) {
        case (token, (accum, count)) => {
          // weight is 0 if token does not have an assigned weight
          val current = accum + tokenWeights.getOrElse(token, 0.0)

          if (current < modThreshold) (current, count + 1) else (current, count)
        }
      }._2
    }

    if (removedSize > tokenSet.size)
      tokenSet.size
    else if (removedSize < 0)
      0
    else
      removedSize

  }

  /**
   * Sort the tokenSet based on token's frequency
   */
  private def sortTokenSet(tokenSet: Seq[String], tokenRank: Broadcast[Map[String, Int]])
  : Seq[String] = {
    tokenSet.map(token => (token, tokenRank.value.getOrElse(token, 0))).toSeq.sortBy(_._2).map(_._1)
  }

  /**
   * Count the number of times that each token shows up in the data
   */

  private def computeTokenCount(data: RDD[(Seq[String])]): collection.Map[String, Int] = {
    data.flatMap{
      case tokenSet =>
        for (x <- tokenSet.distinct) // performance issue
        yield (x, 1)
    }.reduceByKeyLocally(_ + _)
  }

  def broadcastJoin (@transient sc: SparkContext,
                     sampleTable: SchemaRDD,
                     fullTable: SchemaRDD,
                     sampleKey: BlockingKey,
                     fullKey: BlockingKey,
                     threshold: Double) : RDD[(Row,Row)] = {

    // Add record ID into sampleTable: RDD[(Id, (Seq[String],Row)))]
    // Seq[String] is blocking key
    // Id is a unique id assigned for each row
    val sampleTableWithId: RDD[(Long, (Seq[String], Row))] = sampleTable.zipWithUniqueId
      .map(x => (x._2, (sampleKey.tokenSet(x._1), x._1))).cache()
    sampleTableWithId.setName("sampleTableWithId")

    // Materialize tokenCountMap for computing tokenRankMap and tokenWeightMap
    val tokenCountMap = computeTokenCount(sampleTableWithId.map(_._2._1))

    val tokenRankMap: Map[String, Int] = tokenCountMap.toArray.sortBy(_._2).map(_._1).zipWithIndex.toMap
    val tableSize = sampleTable.count()
    val tokenWeightMap = tokenCountMap.map(x => (x._1, math.log10(tableSize / x._2)))


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
      case (row2) =>
        val weightsValue = broadcastWeights.value
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value

        val key2 = fullKey.tokenSet(row2)
        val removedSize = getRemovedSize(key2, threshold, weightsValue)
        val sorted: Seq[String] = sortTokenSet(key2, broadcastRank).dropRight(removedSize)


        sorted.foldLeft(Seq[Long]()) {
          case (a, b) =>
              a ++ broadcastIndexValue.getOrElse(b, Seq())
        }.distinct.map {
          case id =>
            val (key1, row1) = broadcastDataValue(id)
            val similar: Boolean = isSimilar(key1, key2, threshold, weightsValue)
            (key1, row1, similar)
        }.withFilter(_._3).map {
          case (key1, row1, similar) => (row1, row2)
        }
    })
  }
}

