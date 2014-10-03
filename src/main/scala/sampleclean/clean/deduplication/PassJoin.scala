package sampleclean.clean.deduplication
/**
 * Created by juanmanuelsanchez on 9/29/14.
 */

import sampleclean.api.SampleCleanContext

import scala.collection.Seq

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.SparkContext._


class PassJoin extends Serializable {


  // generate even string segments
  def genEvenSeg(seg_str_len: Int, threshold: Int): Seq[(Int,Int)] = {

    val partNum = threshold + 1
    var segLength = seg_str_len / partNum
    var segStartPos = 0

    (0 until partNum).map {pid =>
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
      (segStartPos, segLength)
    }
  }

  // strLength is length of string to be divided into substrings
  def genOptimalSubs(strLength: Int, threshold: Int): Seq[(Int,Int)] = {

    val candidateLengths = strLength - threshold to strLength + threshold
    candidateLengths.flatMap(genSubstrings(_, strLength, threshold)).distinct

  }

  // returns starting position and length
  def genSubstrings(seg_str_len: Int, sub_str_len: Int, threshold: Int): Seq[(Int,Int)] = {
    val partNum = threshold + 1
    // first value is segment starting position, second value is segment length
    val segInfo = genEvenSeg(seg_str_len, threshold)

    (0 until partNum).flatMap {pid =>
      for (stPos:Int <- Seq(0,segInfo(pid)._1 - pid, segInfo(pid)._1 + (sub_str_len - seg_str_len) - (threshold - pid)).max
        to Seq(sub_str_len - segInfo(pid)._2, segInfo(pid)._1 + pid, segInfo(pid)._1 + (sub_str_len - seg_str_len) + (threshold - pid)).min)
      yield (stPos, segInfo(pid)._2)
    }//.slice(0,partNum)

  }

  def broadcastJoin (@transient sc: SparkContext,
                     threshold: Int,
                     fullTable: SchemaRDD,
                     getAttrFull: BlockingKey,
                     sampleTable: SchemaRDD,
                     getAttrSample: BlockingKey): RDD[(Row,Row)] = {


    //Add a record ID into sampleTable. Id is a unique id assigned to each row.
    val sampleTableWithId: RDD[(Long, (String, Row))] = sampleTable.zipWithUniqueId()
      .map(x => (x._2, (getAttrSample.concat(x._1), x._1))).cache()

    //find max string length in sample
    val maxLen = sampleTableWithId.map(_._2._1.length).top(1).toSeq(0)
    println("maxLen " + maxLen)

    // build hashmap for possible substrings and segments according to string length
    val likelyRange = 0 to maxLen + threshold
    val subMap = (0 to maxLen).map(length => (length,genOptimalSubs(length,threshold))).toMap
    val segMap = likelyRange.map(length => (length,genEvenSeg(length,threshold))).toMap


    // Build an inverted index with substrings
    val invertedIndex = sampleTableWithId.flatMap {
      case (id, (string, row)) =>
        val ranges = subMap(string.length)
        val substrings = ranges.map(x => string.substring(x._1, x._1 + x._2))
        substrings.map(x => (x, id))
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))


    //Broadcast sample data to all nodes
    val broadcastIndex: Broadcast[collection.Map[String, Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (String, Row)]] = sc.broadcast(sampleTableWithId.collectAsMap())
    val broadcastSegMap = sc.broadcast(segMap)


    //Generate the candidates whose segments and substrings have overlap, and then verify their edit distance similarity
    fullTable.flatMap({
      case (row1) =>
        val string1 = getAttrFull.concat(row1)
        if (string1.length > maxLen + threshold) List()
        else {
          val broadcastDataValue = broadcastData.value
          val broadcastIndexValue = broadcastIndex.value
          val broadcastSegMapValue = broadcastSegMap.value

          val segments = {
            broadcastSegMapValue(string1.length).map {
              case (stPos, len) => string1.substring(stPos, stPos + len)
            }
          }

          segments.foldLeft(List[Long]()) {
            case (ids, string) =>
              ids ++ broadcastIndexValue.getOrElse(string, List())
          }.distinct.map {
            case id =>
              val (string2, row2) = broadcastDataValue(id)
              val similar = {
                thresholdLevenshtein(string1, string2, threshold) <= threshold
              }
              (string2, row2, similar)
          }.withFilter(_._3).map {
            case (key2, row2, similar) => (row1, row2)
          }
        }
    })


  }

  def broadcastJoin (@transient sc: SparkContext,
                     threshold: Int,
                     fullTable: SchemaRDD,
                     getAttrFull: BlockingKey
                     ): RDD[(Row,Row)] = {


    //Add a record ID into sampleTable. Id is a unique id assigned to each row.
    val tableWithId: RDD[(Long, (String, Row))] = fullTable.zipWithUniqueId()
      .map(x => (x._2, (getAttrFull.concat(x._1), x._1))).cache()

    //find max string length in sample
    val maxLen = tableWithId.map(_._2._1.length).top(1).toSeq(0)
    println("maxLen " + maxLen)

    // build hashmap for possible substrings and segments according to string length
    val likelyRange = 0 to maxLen
    val subMap = likelyRange.map(length => (length,genOptimalSubs(length,threshold))).toMap
    val segMap = likelyRange.map(length => (length,genEvenSeg(length,threshold))).toMap


    // Build an inverted index with substrings
    val invertedIndex = tableWithId.flatMap {
      case (id, (string, row)) =>
        val ranges = subMap(string.length)
        val substrings = ranges.map(x => string.substring(x._1, x._1 + x._2))
        substrings.map(x => (x, id))
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))


    //Broadcast sample data to all nodes
    val broadcastIndex: Broadcast[collection.Map[String, Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (String, Row)]] = sc.broadcast(tableWithId.collectAsMap())
    val broadcastSegMap = sc.broadcast(segMap)


    //Generate the candidates whose segments and substrings have overlap, and then verify their edit distance similarity
    tableWithId.flatMap({
      case (id1, (string1, row1)) =>
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value
        val broadcastSegMapValue = broadcastSegMap.value

        val segments = {
          broadcastSegMapValue(string1.length).map {
            case (stPos, len) => string1.substring(stPos, stPos + len)
          }
        }

        segments.foldLeft(List[Long]()) {
          case (ids, string) =>
            ids ++ broadcastIndexValue.getOrElse(string, List())
        }.distinct.map {
          case id2 =>
            if (id2 >= id1) (null, null, false)
            else {
              val (string2, row2) = broadcastDataValue(id2)
              val similar = {
                thresholdLevenshtein(string1, string2, threshold) <= threshold
              }
              (string2, row2, similar)
            }
        }.withFilter(_._3).map {
          case (key2, row2, similar) => (row1, row2)
          }
    })


  }

  // Given EditD calculator
  def thresholdLevenshtein(_s: String, _t: String, threshold: Int): Int = {
    val (s, t) = if (_s.length > _t.length) (_s, _t) else (_t, _s)
    val slen = s.length
    val tlen = t.length

    var prev = Array.fill[Int](tlen+1)(Int.MaxValue)
    var curr = Array.fill[Int](tlen+1)(Int.MaxValue)
    for (n <- 0 until math.min(tlen+1, threshold+1)) prev(n) = n

    for (row <- 1 until (slen+1)) {
      curr(0) = row
      val min = math.min(tlen+1, math.max(1, row - threshold))
      val max = math.min(tlen+1, row + threshold + 1)

      if (min > 1) curr(min-1) = Int.MaxValue
      for (col <- min until max) {
        curr(col) = if (s(row-1) == t(col-1)) prev(col-1)
        else math.min(prev(col-1), math.min(curr(col-1), prev(col))) + 1
      }
      prev = curr
      curr = Array.fill[Int](tlen+1)(Int.MaxValue)
    }

    prev(tlen)
  }

/*
  // Original C++ Algorithm translated to Scala
  def getSubstringInfo(tau:Int, seg_str_len:Int, sub_str_len:Int): Unit = {
    var subInfo: Seq[(Int,Int)] = Seq()
    val partNum = tau + 1; // number of segments, should be tau + 1

    // calculate segment information
    var segLen = Seq[Int]() // the length of each segment
    var segPos = Seq[Int]() // the start position of each segment
    segPos = segPos :+ 0 // the start position of the first segment should be 0
    segLen = segLen :+ (seg_str_len / partNum) // the length of the first segment
    for (pid <- 1 until partNum) { // calculate the segment information
      segPos = segPos :+ (segPos(pid-1) + segLen(pid-1))
      if (pid == (partNum - seg_str_len % partNum)) // almost evenly partition
        segLen = segLen :+ (segLen(pid-1) + 1)
      else
        segLen = segLen :+ segLen(pid-1)
    }

    // calculate substring information
    // enumerate the valid substrings with respect to seg_str_len, sub_str_len and pid
    for (pid <- 0 until partNum) {
      // stPos is the begin position of a substring and the length of the substring should equal to the length of the segment
      for (stPos <- Seq(0,segPos(pid) - pid, segPos(pid) + (sub_str_len - seg_str_len) - (tau - pid)).max
      to Seq(sub_str_len - segLen(pid), segPos(pid) + pid, segPos(pid) + (sub_str_len - seg_str_len) + (tau - pid)).min)
      subInfo = subInfo :+ (stPos, segLen(pid))
    }

    // print
    for (i <- 0 until partNum) {
      println("segment" , segPos(i) , segLen(i))
    }
    for (i <- 0 until subInfo.length) {
      println("substring", subInfo(i)._1, subInfo(i)._2)
    }

  }*/

}



object PassJoin {
  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.setAppName("PassJoinDriver")
    conf.setMaster("local[4]")
    conf.set("spark.executor.memory", "4g")

    val sc = new SparkContext(conf)
    val scc = new SampleCleanContext(sc)

    val hiveContext = scc.getHiveContext()
    //hiveContext.hql("DROP TABLE IF EXISTS restaurant")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS restaurant(id String,name String,address String,city String,type String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH '/Users/juanmanuelsanchez/Documents/sampleCleanData/restaurant.csv' OVERWRITE INTO TABLE restaurant")
    scc.closeHiveSession()

    scc.initialize("restaurant","restaurant_sample",1)

    val blockedCols = List("name", "address")
    val sampleTableName = "restaurant_sample"

    val sampleTable = scc.getCleanSample(sampleTableName).cache
    val fullTable = scc.getFullTable(sampleTableName).cache

    val sampleTableColMapper = scc.getSampleTableColMapper(sampleTableName)
    val fullTableColMapper = scc.getFullTableColMapper(sampleTableName)
    val genKeyFull = BlockingKey(fullTableColMapper(blockedCols), WordTokenizer())
    val genKeySample = BlockingKey(sampleTableColMapper(blockedCols), WordTokenizer())

    println("sample count " + sampleTable.count())
    println("full count " + fullTable.count())

    val join = new PassJoin
    //val joined = join.broadcastJoin(sc,2,fullTable,genKeyFull,sampleTable, genKeySample).cache()
    val joined = join.broadcastJoin(sc,8,fullTable,genKeyFull).cache()

    println(joined.count())
    println(joined.first())
  }
}