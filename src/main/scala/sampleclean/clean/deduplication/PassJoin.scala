package sampleclean.clean.deduplication
/**
 * Created by juanmanuelsanchez on 9/29/14.
 */


import scala.collection.Seq
import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.SparkContext._

/**
 * This class represents a similarity join using the edit distance similarity measure.
 */
class PassJoin extends Serializable {

  /**
   * Compares two strings and returns whether they are similar or not, relative to the specified threshold.
   * @param _s first string
   * @param _t second string
   * @param threshold specified threshold (integer)
   */
  def isSimilar(_s: String, _t: String, threshold: Int): Boolean = {
    thresholdLCS(_s, _t, threshold) <= threshold
  }

  /**
   * Calculates the Edit Distance by calculating the Levenshtein distance between two strings.
   * @param _s first string
   * @param _t secind string
   * @param threshold specified threshold (integer)
   */
  def thresholdLevenshtein(_s: String, _t: String, threshold: Int): Int = {
    val (s, t) = if (_s.length > _t.length) (_s, _t) else (_t, _s)
    val slen = s.length
    val tlen = t.length

    var prev = Array.fill[Int](tlen + 1)(Int.MaxValue)
    var curr = Array.fill[Int](tlen + 1)(Int.MaxValue)
    for (n <- 0 until math.min(tlen + 1, threshold + 1)) prev(n) = n

    for (row <- 1 until (slen + 1)) {
      curr(0) = row
      val min = math.min(tlen + 1, math.max(1, row - threshold))
      val max = math.min(tlen + 1, row + threshold + 1)

      if (min > 1) curr(min - 1) = Int.MaxValue
      for (col <- min until max) {
        curr(col) = if (s(row - 1) == t(col - 1)) prev(col - 1)
        else math.min(prev(col - 1), math.min(curr(col - 1), prev(col))) + 1
      }
      prev = curr
      curr = Array.fill[Int](tlen + 1)(Int.MaxValue)
    }

    prev(tlen)
  }

  /**
   * Optimized version of the Levenshtein distance.
   * @param _s first string
   * @param _t second string
   * @param threshold specified threshold (integer)
   */
  def thresholdLCS(_s: String, _t: String, threshold: Int): Int = {
    val (s, t) = if (_s.length > _t.length) (_s, _t) else (_t, _s)
    val n = s.length
    val m = t.length

    if (n - m > threshold) return threshold + 1

    var V = new Array[Array[Int]](threshold * 3 + 2)
    for (i <- 0 until V.length)
      V(i) = Array.fill[Int](2)(Int.MinValue)

    V(-1 + threshold + 1)(-1 & 1) = 0

    for (p <- 0 until threshold + 1) {
      val f = p & 1
      val g = f ^ 1

      for (k <- threshold + 1 - p until threshold + 1 + p + 1) {
        V(k)(f) = math.max(math.max(V(k)(g), V(k + 1)(g)) + 1, V(k - 1)(g))
        val d = k - threshold - 1
        if (V(k)(f) >= 0 && V(k)(f) + d >= 0)
          while (V(k)(f) < n && V(k)(f) + d < m && s(V(k)(f)) == t(V(k)(f) + d))
            V(k)(f) += 1
      }
      if (V(m - n + threshold + 1)(f) >= n) return p
    }
    threshold + 1

  }

  /**
   * Generates string segments for faster pair filtering, as per PassJoin algorithm.
   * @param seg_str_len length of string to be segmented
   *@param threshold specified threshold
   */
  def genEvenSeg(seg_str_len: Int, threshold: Int): Seq[(Int, Int, Int, Int)] = {
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
  def genOptimalSubs(strLength: Int, threshold: Int, selfJoin: Boolean = false): Seq[(Int, Int, Int, Int)] = {

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
  def genSubstrings(seg_str_len: Int, sub_str_len: Int, threshold: Int): Seq[(Int, Int, Int, Int)] = {
    val partNum = threshold + 1
    // first value is segment starting position, second value is segment length
    val segInfo = genEvenSeg(seg_str_len, threshold)

    (0 until partNum).flatMap { pid =>
      for (stPos: Int <- Seq(0, segInfo(pid)._2 - pid, segInfo(pid)._2 + (sub_str_len - seg_str_len) - (threshold - pid)).max
        to Seq(sub_str_len - segInfo(pid)._3, segInfo(pid)._2 + pid, segInfo(pid)._2 + (sub_str_len - seg_str_len) + (threshold - pid)).min)
      yield (pid, stPos, segInfo(pid)._3, seg_str_len)
    }

  }

  /**
   * Performs a similarity join between two data sets using the Edit Distance
   * similarity measure, the PassJoin algorithm and a broadcasting procedure.
   * @param sc SparkContext
   * @param threshold maximum Edit Distance that will be used for comparison
   * @param fullTable large table
   * @param getAttrFull strategy that will be used for column concatenation in large table
   * @param sampleTable small table
   * @param getAttrSample strategy that will be used for column concatenation in small table
   */
  def broadcastJoin(@transient sc: SparkContext,
                    threshold: Double,
                    fullTable: RDD[Row],
                    getAttrFull: BlockingKey,
                    sampleTable: RDD[Row],
                    getAttrSample: BlockingKey): RDD[(Row, Row)] = {


    val intThreshold = threshold.toInt
    //Add a record ID into sampleTable. Id is a unique id assigned to each row.
    val sampleTableWithId: RDD[(Long, (String, Row))] = sampleTable.zipWithUniqueId()
      .map(x => (x._2, (getAttrSample.concat(x._1), x._1))).cache()


    //find max string length in sample
    val maxLen = sampleTableWithId.map(_._2._1.length).top(1).toSeq(0)

    // build hashmap for possible substrings and segments according to string length
    val likelyRange = 0 to maxLen + intThreshold
    val subMap = (0 to maxLen).map(length => (length, genOptimalSubs(length, intThreshold))).toMap
    val segMap = likelyRange.map(length => (length, genEvenSeg(length, intThreshold))).toMap


    // Build an inverted index with segments
    val invertedIndex = sampleTableWithId.flatMap {
      case (id, (string, row)) =>
        val ranges = segMap(string.length)
        val segments = ranges.map(x => (x._1, string.substring(x._2, x._2 + x._3), x._4))
        segments.map(x => (x, id))
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct)).cache()



    //Broadcast sample data to all nodes
    val broadcastIndex: Broadcast[collection.Map[(Int, String, Int), Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (String, Row)]] = sc.broadcast(sampleTableWithId.collectAsMap())
    val broadcastSubMap = sc.broadcast(subMap)


    //Generate the candidates whose segments and substrings have overlap, and then verify their edit distance similarity
    fullTable.flatMap({
      case (row1) =>
        val string1 = getAttrFull.concat(row1)
        if (string1.length > maxLen + intThreshold) List()
        else {
          val broadcastDataValue = broadcastData.value
          val broadcastIndexValue = broadcastIndex.value
          val broadcastSubMapValue = broadcastSubMap.value

          val substrings = broadcastSubMapValue(string1.length).map {
            case (pid, stPos, len, segLen) => (pid, string1.substring(stPos, stPos + len), segLen)
          }

          substrings.foldLeft(Seq[Long]()) {
            case (ids, (pid: Int, string: String, segLen: Int)) =>
              ids ++ broadcastIndexValue.getOrElse((pid, string, segLen), List()).distinct
          }.map {
            case id2 =>
              val (string2, row2) = broadcastDataValue(id2)
              val similar = {
                isSimilar(string1, string2, intThreshold)
              }
              (string2, row2, similar)

          }.withFilter(_._3).map {
            case (key2, row2, similar) => (row1, row2)
          }
        }
    }).distinct()


  }

  /**
   * Performs a similarity self-join on a data set using the Edit Distance
   * similarity measure, the PassJoin algorithm and a broadcasting procedure.
   * @param sc SparkContext
   * @param threshold maximum Edit Distance that will be used for comparison
   * @param fullTable table
   * @param getAttrFull strategy that will be used for column concatenation
   */
  def broadcastJoin(@transient sc: SparkContext,
                    threshold: Double,
                    fullTable: RDD[Row],
                    getAttrFull: BlockingKey
                     ): RDD[(Row, Row)] = {


    val intThreshold = threshold.toInt
    //Add a record ID into sampleTable. Id is a unique id assigned to each row.
    val tableWithId: RDD[(Long, (String, Row))] = fullTable.zipWithUniqueId()
      .map(x => (x._2, (getAttrFull.concat(x._1), x._1))).cache()


    //find max string length in sample
    val maxLen = tableWithId.map(_._2._1.length).top(1).toSeq(0)

    // build hashmap for possible substrings and segments according to string length
    val likelyRange = 0 to maxLen
    val subMap = likelyRange.map(length => (length, genOptimalSubs(length, intThreshold, true))).toMap
    val segMap = likelyRange.map(length => (length, genEvenSeg(length, intThreshold))).toMap


    // Build an inverted index with segments
    val invertedIndex = tableWithId.flatMap {
      case (id, (string, row)) =>
        val ranges = segMap(string.length)
        val substrings = ranges.map(x => (x._1, string.substring(x._2, x._2 + x._3), x._4))

        substrings.map(x => (x, id))
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct)).cache()


    //Broadcast sample data to all nodes
    val broadcastIndex: Broadcast[collection.Map[(Int, String, Int), Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (String, Row)]] = sc.broadcast(tableWithId.collectAsMap())
    val broadcastSubMap = sc.broadcast(subMap)


    //Generate the candidates whose segments and substrings have overlap, and then verify their edit distance similarity
    tableWithId.flatMap({
      case (id1, (string1, row1)) =>
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value
        val broadcastSubMapValue = broadcastSubMap.value

        val substrings = broadcastSubMapValue(string1.length).map {
          case (pid, stPos, len, segLen) => (pid, string1.substring(stPos, stPos + len), segLen)
        }

        substrings.foldLeft(Seq[Long]()) {
          case (ids, (pid: Int, string: String, segLen: Int)) =>
            ids ++ broadcastIndexValue.getOrElse((pid, string, segLen), List()).distinct
        }.map {
          case id2 =>
            val (string2, row2) = broadcastDataValue(id2)
            if (string1.length < string2.length) (null, null, false)
            else if (string1.length == string2.length && id2 >= id1) {
              (null, null, false)
            }
            else {
              val similar = {
                isSimilar(string1, string2, intThreshold)
              }
              (string2, row2, similar)
            }
        }.withFilter(_._3).map {
          case (key2, row2, similar) => (row1, row2)
        }
    }).distinct()


  }

}