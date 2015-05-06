package sampleclean.clean.deduplication.join

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer

import scala.collection.Seq

/**
 * A PassJoin is an implementation of a Similarity Join optimized
 * for String comparisons. Currently, this algorithm
 * is only supported by the EditBlocking Similarity Featurizer.
 * In a distributed environment, this optimization involves
 * broadcasting a series of maps to each node.
 *
 * '''Note:''' because the algorithm may collect large RDDs into maps by using
 * driver memory, java heap problems could arise. In this case, it is
 * recommended to increase allocated driver memory through Spark configuration
 * spark.driver.memory
 *
 * @param sc Spark Context
 * @param featurizer Similarity Featurizer optimized for [[PassJoin]]
 */
class PassJoin( @transient sc: SparkContext,
               featurizer: AnnotatedSimilarityFeaturizer) extends SimilarityJoin(sc,featurizer,false) {

  @Override
  override def join(rddA: RDD[Row],
                    rddB: RDD[Row],
                    sampleA:Boolean = false): RDD[(Row,Row)] = {

    println("[SampleClean] Starting Broadcast Pass Join")

    if (!featurizer.usesStringPrefixFiltering) {
      super.join(rddA, rddB, sampleA)
    }

    else {

      val intThreshold = featurizer.threshold.toInt

      var largeTableSize = rddB.count()
      var smallTableSize = rddA.count()
      var smallTable = rddA
      var largeTable = rddB

      val isSelfJoin = !sampleA

      var smallTableWithId: RDD[(String, (Seq[String], String, Row))] = null

      //Add a record ID into smallTable. Id is a unique id assigned to each row.
      if(!sampleA)
      {
        smallTableWithId = smallTable.zipWithUniqueId()
          .map(x => {
          val block = featurizer.tokenizer.tokenize(x._1, featurizer.getCols(false))
          (x._2.toString, (block, block.mkString(" "), x._1))
        }).cache()
      }
      else
      {
        smallTableWithId = smallTable.map(x => {
          val block = featurizer.tokenizer.tokenize(x, featurizer.getCols(false))
          (x(0).asInstanceOf[String], (block, block.mkString(" "), x))
        }).cache()
      }


      //find max string length in sample
      val maxLen = smallTableWithId.map(_._2._2.length).top(1).toSeq(0)

      // build hashmap for possible substrings and segments according to string length
      val likelyRange = 0 to maxLen
      val subMap = likelyRange.map(length => (length, genOptimalSubs(length, intThreshold))).toMap
      val segMap = {
        if (isSelfJoin)
          likelyRange.map(length => (length, genEvenSeg(length, intThreshold))).toMap
        else
          (0 to maxLen + intThreshold).map(length => (length, genEvenSeg(length, intThreshold))).toMap
      }


      // Build an inverted index with segments
      val invertedIndex = smallTableWithId.flatMap {
        case (id, (list, string, row)) =>
          val ranges = segMap(string.length)
          val segments = ranges.map(x => (x._1, string.substring(x._2, x._2 + x._3), x._4))
          segments.map(x => (x, id))
      }.groupByKey().map(x => (x._1, x._2.toSeq.distinct)).cache()



      //Broadcast sample data to all nodes
      val broadcastIndex: Broadcast[collection.Map[(Int, String, Int), Seq[String]]] = sc.broadcast(invertedIndex.collectAsMap())
      val broadcastData: Broadcast[collection.Map[String, (Seq[String], String, Row)]] = sc.broadcast(smallTableWithId.collectAsMap())
      val broadcastSubMap = sc.broadcast(subMap)

      val scanTable: RDD[(String, (Seq[String], String, Row))] = {
        if (isSelfJoin) smallTableWithId
        else {
          largeTable.map(row => {
            val key = featurizer.tokenizer.tokenize(row,featurizer.getCols(false))
            (row(0).asInstanceOf[String], (key,key.mkString(" "), row))
          })
        }
      }

      //Generate the candidates whose segments and substrings have overlap, and then verify their edit distance similarity
      scanTable.flatMap({
        case (id1, (key1, string1, row1)) =>
          if (string1.length > maxLen + intThreshold && !isSelfJoin) List()
          else {
            val broadcastDataValue = broadcastData.value
            val broadcastIndexValue = broadcastIndex.value
            val broadcastSubMapValue = broadcastSubMap.value

            val substrings = broadcastSubMapValue(string1.length).map {
              case (pid, stPos, len, segLen) => (pid, string1.substring(stPos, stPos + len), segLen)
            }

            substrings.foldLeft(Seq[String]()) {
              case (ids, (pid: Int, string: String, segLen: Int)) =>
                ids ++ broadcastIndexValue.getOrElse((pid, string, segLen), List()).distinct
            }.map {
              case id2 =>
                val (key2, string2, row2) = broadcastDataValue(id2)
                if (string1.length < string2.length && isSelfJoin) (null, null, false)
                else if (string1.length == string2.length && id2.toString >= id1.toString && isSelfJoin) {
                  (null, null, false)
                }
                else if (id2.toString == id1.toString) {
                  (null, null, false)
                }
                else {
                  val similar = {
                    featurizer.optimizedSimilarity(key1, key2, intThreshold, Map[String, Double]())._1
                  }
                  (string2, row2, similar)
                }

            }.withFilter(_._3).map {
              case (key2, row2, similar) => (row1, row2)
            }
          }
      }).distinct()
    }
  }

  /**
   * Generates string segments for faster pair filtering, as per PassJoin algorithm.
   * @param seg_str_len length of string to be segmented
   *@param threshold specified threshold
   */
  private def genEvenSeg(seg_str_len: Int, threshold: Int): Seq[(Int, Int, Int, Int)] = {
    val partNum = threshold + 1
    var segLength = seg_str_len / partNum
    var segStartPos = 0

    (0 until partNum).map { pid =>
      if (pid == 0) (segStartPos, segLength)
      else {
        segStartPos += segLength
        segLength = {
          if (pid == (partNum - seg_str_len % partNum)) // almost evenly partition
            segLength + 1
          else
            segLength
        }
      }
      (pid, segStartPos, segLength, seg_str_len)
    }
  }

  /**
   * Generates optimal tuples with substring information for a given string length.
   * This is part of the PassJoin algorithm.
   * @param strLength string length
   * @param threshold specified threshold
   * @param selfJoin if true, assumes a self-join is being performed
   */
  private def genOptimalSubs(strLength: Int, threshold: Int, selfJoin: Boolean = false): Seq[(Int, Int, Int, Int)] = {

    val candidateLengths = {
      if (!selfJoin) strLength - threshold to strLength + threshold
      else strLength - threshold to strLength
    }
    candidateLengths.flatMap(genSubstrings(_, strLength, threshold)).distinct

  }


  /**
   * Generates tuples with substring information for a given string length.
   * This is part of the PassJoin algorithm.
   * @param seg_str_len segment length
   * @param sub_str_len substring length
   * @param threshold specified threshold
   */
  private def genSubstrings(seg_str_len: Int, sub_str_len: Int, threshold: Int): Seq[(Int, Int, Int, Int)] = {
    val partNum = threshold + 1
    // first value is segment starting position, second value is segment length
    val segInfo = genEvenSeg(seg_str_len, threshold)

    (0 until partNum).flatMap { pid =>
      for (stPos: Int <- Seq(0, segInfo(pid)._2 - pid, segInfo(pid)._2 + (sub_str_len - seg_str_len) - (threshold - pid)).max
        to Seq(sub_str_len - segInfo(pid)._3, segInfo(pid)._2 + pid, segInfo(pid)._2 + (sub_str_len - seg_str_len) + (threshold - pid)).min)
      yield (pid, stPos, segInfo(pid)._3, seg_str_len)
    }

  }
}
