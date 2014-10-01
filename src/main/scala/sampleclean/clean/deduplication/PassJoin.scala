package sampleclean.clean.deduplication

import org.apache.spark.sql.hive.HiveContext
import sampleclean.api.{SampleCleanAQP, SampleCleanContext}

import scala.collection.{mutable, Seq}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.SparkContext._

/**
 * Created by juanmanuelsanchez on 9/29/14.
 */

class PassJoin extends Serializable {


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
      for (stPos:Int <- math.max(segInfo(pid)._1 - pid, segInfo(pid)._1 + (sub_str_len - seg_str_len) - (threshold - pid))
        to math.min(segInfo(pid)._1 + pid, segInfo(pid)._1 + (sub_str_len - seg_str_len) + (threshold - pid))) yield (stPos, segInfo(pid)._2)
    }.slice(0,partNum)

  }

  def broadcastJoin (@transient sc: SparkContext,
                     threshold: Int,
                     fullTable: SchemaRDD,
                     sampleTable: SchemaRDD): RDD[(Row,Row)] = {

    //find max string length in sample
    val maxLength:Int = sampleTable.map(row => row.getString(0).length).top(1).toSeq(0)

    //Add a record ID into sampleTable. Id is a unique id assigned to each row.
    val sampleTableWithId: RDD[(Long, (String, Row))] = sampleTable.zipWithUniqueId()
      //row only contains one column
      .map(x => (x._2, (x._1.getString(0), x._1))).cache()

    // build hashmap for possible substrings according to string length
    val likelyRange = 0 to maxLength + threshold
    val segMap = likelyRange.map(length => (length,genOptimalSubs(length,threshold))).toMap


    // Build an inverted index with segments
    val invertedIndex = sampleTableWithId.flatMap {
      case (id, (string, row)) =>
        val ranges = genOptimalSubs(string.length, threshold)
        val segments = ranges.map(x => string.substring(x._1, x._1 + x._2))
        segments.map(x => (x, id))
    }.groupByKey().map(x => (x._1, x._2.toSeq.distinct))


    //Broadcast sample data to all nodes
    val broadcastIndex: Broadcast[collection.Map[String, Seq[Long]]] = sc.broadcast(invertedIndex.collectAsMap())
    val broadcastData: Broadcast[collection.Map[Long, (String, Row)]] = sc.broadcast(sampleTableWithId.collectAsMap())
    val broadcastMaxLen = sc.broadcast(maxLength)


    //Generate the candidates whose substrings have overlap, and then verify their edit distance similarity
    fullTable.flatMap({
      case (row1) =>
        val broadcastDataValue = broadcastData.value
        val broadcastIndexValue = broadcastIndex.value

        val string1 = row1.toString()
        val segments = genEvenSeg(string1.length, threshold).map {
          case (stPos, len) => string1.substring(stPos, stPos + len)
        }

        segments.foldLeft(List[Long]()) {
          case (ids, string) =>
            ids ++ broadcastIndexValue.getOrElse(string, List())
        }.distinct.map {
          case id =>
            val (string2, row2) = broadcastDataValue(id)
            val similar = {
              if (string2.length > broadcastMaxLen.value + threshold) false
              else thresholdLevenshtein(string1, string2, threshold) <= threshold
            }
            (string2, row2, similar)
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

  // Original Algorithm translated to Scala
  // Returns negative indices?? e.g. try (2,2,2) or (5,4,4)
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
      for (stPos <- math.max(segPos(pid) - pid, segPos(pid) + (sub_str_len - seg_str_len) - (tau - pid))
      to math.min(segPos(pid) + pid, segPos(pid) + (sub_str_len - seg_str_len) + (tau - pid)))
      subInfo = subInfo :+ (stPos, segLen(pid))
    }

    // print
    for (i <- 0 until partNum) { // <- Jiannan: # of segments = # of substrings??
      println("segment" , segPos(i) , segLen(i))
      println("substring", subInfo(i)._1, subInfo(i)._2)
    }
    println("number of substrings",subInfo.length)

  }

}



object PassJoin {
  def main(args: Array[String]) = {

    val conf = new SparkConf();
    conf.setAppName("PassJoinDriver");
    conf.setMaster("local[4]");
    conf.set("spark.executor.memory", "4g");

    val sc = new SparkContext(conf);
    val scc = new SampleCleanContext(sc);

    val hiveContext = scc.getHiveContext();
    //hiveContext.hql("DROP TABLE IF EXISTS restaurant")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS restaurant(id String,name String,address String,city String,type String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH '/Users/juanmanuelsanchez/Documents/sampleCleanData/restaurant.csv' OVERWRITE INTO TABLE restaurant")
    scc.closeHiveSession()

    scc.initialize("restaurant","restaurant_sample",0.1)

    def getFullTableAttr(sampleName: String, attr: String):SchemaRDD = {
      val hiveContext = new HiveContext(sc)
      hiveContext.hql(scc.qb.buildSelectQuery(List(attr),scc.getParentTable(scc.qb.getCleanSampleName(sampleName))))
    }

    def getCleanSampleAttr(tableName: String, attr: String):SchemaRDD = {
      val hiveContext = new HiveContext(sc)
      hiveContext.hql(scc.qb.buildSelectQuery(List(attr),scc.qb.getCleanSampleName(tableName)))
    }

    val sampleTable = getCleanSampleAttr("restaurant_sample","name")
    val fullTable = getFullTableAttr("restaurant_sample","name")

    println(sampleTable.first())
    println(sampleTable.count())
    println(fullTable.first())
    println(fullTable.count())

    val join = new PassJoin
    val joined = join.broadcastJoin(sc,2,fullTable,sampleTable)

    println(joined.count())
    println(joined.collect())
  }
}