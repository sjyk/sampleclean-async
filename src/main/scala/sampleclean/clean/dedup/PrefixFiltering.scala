package sampleclean.clean.dedup


import scala.collection.immutable.Map
import scala.reflect.ClassTag
import scala.collection.Seq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._


trait PrefixFiltering extends Serializable {

  // return true if tokenSet1 and tokenSet2 are similar; otherwise, return false
  def isSimilar(tokenSet1: Seq[String], tokenSet2: Seq[String], threshold: Double): Boolean

  // compute the number of tokens that can be removed from the tokenSet
  def getRemovedSize(tokenSetSize: Int, threshold: Double): Int



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
      threshold: Double)
      : RDD[(Row,Row)] = {

    // Add record ID into sampleTable: RDD[(Id, (Seq[String],Row)))]
    // Seq[String] is blocking key
    // Id is a unique id assigned for each row
    val sampleTableWithId: RDD[(Long, (Seq[String], Row))] = sampleTable.zipWithUniqueId
      .map(x => (x._2, (sampleKey.tokenSet(x._1), x._1))).cache()
    sampleTableWithId.setName("sampleTableWithId")


    /*
    mapPartitionsWithIndex {
      case (partition, iter) =>
        for ((x, i) <- iter.zipWithIndex)
        yield (partition * 10000000 + i, x)
    }.cache()
    sampleDataId.setName("sampleDataId")
    */

    // Set a global order to all tokens based on their frequencies
    // ??: We may optimize this phase by first collecting as map, and then sort the map
    val tokenRankMap: Map[String, Int] = computeTokenCount(sampleTableWithId.map(_._2._1)).toSeq
      .sortBy(_._2).map(_._1).zipWithIndex.toMap


    // .map(x => (x._2, x._1)).sortByKey().map(_._2) // Get a ranked list of tokens
    // .collect().zipWithIndex.toMap // Generate a rank id for each token
    //tokenRankMap.foreach(println)

    val broadcastRank = sc.broadcast(tokenRankMap)


    // Build an inverted index for the prefixes of sample data
    //RDD[(K, Seq[Int])]
    val invertedIndex = sampleTableWithId.flatMap {
      case (id, (tokens, value)) =>
        val sorted = sortTokenSet(tokens, broadcastRank)
        //val sorted = sortTokenSet(tokenSet, broadcastRank)//.dropRight(threshold-1)
        //sorted.foreach(println)
        for (x <- sorted)
          yield (x, id)
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))


    //Broadcast sample data to all nodes
    val broadcastIndex: Broadcast[collection.Map[String, Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (Seq[String], Row)]] = sc.broadcast(sampleTableWithId.collectAsMap())


    //Generate the candidates whose prefixes have overlap, and then verify their overlap similarity
    fullTable.flatMap({
      case (row2) =>
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value

        val key2 = fullKey.tokenSet(row2)
        val removedSize = getRemovedSize(key2.size, threshold)
        val sorted: Seq[String] = sortTokenSet(key2, broadcastRank).dropRight(removedSize)

        //val sorted = tokens2.map(token => (token, broadcastRank.get(token).get)).sortBy(_._2).map(_._1)
        //for (i <- 1 to 10) yield ((tokens2, tokens2, 1), (value2, value2))

       sorted.foldLeft(Seq[Long]()) {
          case (a, b) =>
            a ++ broadcastIndexValue.getOrElse(b, Seq())
       }.distinct.map {
          case id =>
              val (key1, row1) = broadcastDataValue(id)
              val similar: Boolean = isSimilar(key1, key2, threshold)
              (key1, row1, similar)
        }.withFilter(_._3).map {
          case (key1, row1, similar) => (row1, row2)
        }
    })
  }
}
