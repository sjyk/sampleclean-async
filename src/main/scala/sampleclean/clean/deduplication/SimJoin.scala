/**
 * Created by jnwang on 3/25/14.
 */

package sampleclean.clean.deduplication

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.Seq
import scala.reflect.ClassTag

import java.io._


/**
 * This class represents a similarity join based on the Jaccard similarity measure
 */
class JaccardJoin extends PrefixFiltering{ 

  /**
   * Returns true if two token lists are similar; otherwise, return false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   */
  def isSimilar(tokens1: Seq[String], tokens2: Seq[String], threshold: Double): Boolean = {

    //Length Filtering
    if (tokens1.size < tokens2.size)
      if (tokens1.size < tokens2.size*threshold) false
    else
      if (tokens2.size < tokens1.size*threshold) false

    val intersectionSize = tokens1.intersect(tokens2).size
    val unionSize = tokens1.size + tokens2.size - intersectionSize

    if (unionSize == 0)
      false
    else
      intersectionSize.toDouble / unionSize + 1e-6 >= threshold
  }

  /**
   * Computes the number of tokens that can be removed from the token list.
   * @param tokenSetSize  size of the token list.
   * @param threshold specified threshold.
   */
  def getRemovedSize(tokenSetSize: Int, threshold: Double): Int = {
    val removedSize = (tokenSetSize*threshold - 1e-6).ceil.toInt-1
    if (removedSize > tokenSetSize )
      tokenSetSize
    else if (removedSize < 0)
      0
    else
      removedSize
  }
}

/**
 * This class represents a similarity join based on the overlap between the two lists
 */
class OverlapJoin extends PrefixFiltering{

  /**
   * Returns true if two token lists are similar; otherwise, return false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   */
  def isSimilar(tokens1: Seq[String], tokens2: Seq[String], threshold: Double): Boolean = {

    // Length Filtering
    if (tokens1.size < tokens2.size)
      if (tokens1.size < threshold) false
    else
      if (tokens2.size < threshold) false
    tokens1.intersect(tokens2).size >= threshold
  }

  /**
   * Computes the number of tokens that can be removed from the token list.
   * @param tokenSetSize  size of the token list.
   * @param threshold specified threshold.
   */
  def getRemovedSize(tokenSetSize: Int, threshold: Double): Int = {
    val removedSize = (threshold - 1e-6).ceil.toInt-1
    if (removedSize > tokenSetSize)
      tokenSetSize
    else if (removedSize < 0)
      0
    else
      removedSize
  }
}

/**
 * This class represents a similarity join based on the Dice similarity measure
 */
class DiceJoin extends PrefixFiltering{

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   */
  def isSimilar(tokens1: Seq[String], tokens2: Seq[String], threshold: Double): Boolean = {
    val sumLen = tokens1.size+tokens2.size

    //Length Filtering
    if (tokens1.size < tokens2.size)
      if (2*tokens1.size < sumLen*threshold) false
    else
      if (2*tokens2.size < sumLen*threshold) false

    val intersectionSize = tokens1.intersect(tokens2).size

    if (sumLen == 0)
      false
    else
      2 * intersectionSize.toDouble / sumLen >= threshold

  }

  /**
   * Computes the number of tokens that can be removed from the token list.
   * @param tokenSetSize  size of the token list.
   * @param threshold specified threshold.
   */
  def getRemovedSize(tokenSetSize: Int, threshold: Double): Int = {
    if (threshold == 2)
      0
    else {
      val removedSize = (tokenSetSize * threshold / (2 - threshold) - 1e-6).ceil.toInt - 1
      if (removedSize > tokenSetSize)
        tokenSetSize
      else if (removedSize < 0)
        0
      else
        removedSize
    }
  }
}

/**
 * This class represents a similarity join based on the Cosine similarity measure
 */
class CosineJoin extends PrefixFiltering{

  /**
   * Returns true if tokenSet1 and tokenSet2 are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   */
  def isSimilar(tokens1: Seq[String], tokens2: Seq[String], threshold: Double): Boolean = {

    val sqrtLen = math.sqrt(tokens1.size * tokens2.size)

    //Length Filtering
    if (tokens1.size < tokens2.size)
      if (tokens1.size < sqrtLen*threshold) false
    else
      if (tokens2.size < sqrtLen*threshold) false

    val intersectionSize = tokens1.intersect(tokens2).size

    if (sqrtLen == 0)
      false
    else
      intersectionSize.toDouble / sqrtLen >= threshold
  }

  /**
   * Computes the number of tokens that can be removed from the token list.
   * @param tokenSetSize  size of the token list.
   * @param threshold specified threshold.
   * @return
   */
  def getRemovedSize(tokenSetSize: Int, threshold: Double): Int = {
    val removedSize = (tokenSetSize * math.pow(threshold, 2) - 1e-6).ceil.toInt - 1
    if (removedSize > tokenSetSize)
      tokenSetSize
    else if (removedSize < 0)
      0
    else
      removedSize

  }

}



/*
class SimJoin (
    @transient private var sc: SparkContext
  ) extends Serializable {

  def join[K: ClassTag, V:ClassTag](
      sampleData: RDD[((Seq[K], V), Long)],
      fullData: RDD[((Seq[K], V), Long)],
      simfunc: String,
      threshold: Double,
      broadcast: Boolean = true)
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



    }

  }


}
*/

/*
object SimJoin {
  def main(args: Array[String]) {

    val conf = new SparkConf()
                    .setAppName("SimJoin5")
                    //.setMaster("spark://Juans-MacBook-Air.local:7077")
                    //.setMaster("local[4]")
                    //.set("spark.executor.memory", "2g")
                    .set("spark.storage.blockManagerSlaveTimeoutMs", "100000")


    val sc = new SparkContext(conf)

    val fullData = sc.textFile("/user/root/expanded", 9).map(x => (BlockingKey.tokenSet(x, " +").toSeq, 1)).zipWithIndex()
    //val fullData = sc.textFile("file:/Users/juanmanuelsanchez/Documents/sampleCleanData/problem.txt", 8).map(x => (x.split(" +").toSeq, 1)).zipWithIndex()

    //val sampleData = fullData.sample(false, args(0).toDouble, 6)
    val sampleData = fullData.sample(false, 1, 6)



    println("fullCount")
    println(fullData.count())

    println("sampleCount")
    println(sampleData.count())



    val simJoin = new SimJoin(sc)
    //var joined = simJoin.join(sampleData, fullData, "Jaccard", args(1).toDouble)
    val joined = simJoin.join(sampleData, fullData, "Jaccard", 0.8).map(x => (x._1, x._2)).cache().setName("joined")

    println(joined.count())
    println(joined.first())

    //testing
    val test_join = sc.textFile("/user/root/testExpanded").map(x => x.split(" ").toSeq).map(x => ((x(1).toLong, x(2).toLong), 1))
    //val test_join = sc.textFile("/Users/juanmanuelsanchez/Documents/sampleCleanData/jaccard_0.8_clean").map(x => ((x(1).toLong, x(2).toLong), 1))
    val result = joined.map(x => (x._1._2, x._1._1))



    var fstream = new FileWriter("/testScript/control.txt", false)
    var out = new BufferedWriter(fstream)
    for (pair <- test_join.collect.toSeq) yield {
      out.write(pair.toString())
      out.newLine()
    }
    out.close()

    fstream = new FileWriter("/testScript/result.txt", false)
    out = new BufferedWriter(fstream)
    for (pair <- result.collect.toSeq) yield {
      out.write(pair.toString())
      out.newLine()
    }
    out.close()

/*
    fstream = new FileWriter("/testScript/fulldata.txt", false)
    out = new BufferedWriter(fstream)
    for (record <- fullData.map(x => (x._2, x._1._1)).collect.toSeq) yield {
      out.write(record.toString())
      out.newLine()
    }
    out.close()*/





/*
    // Test code for Feature Vector
    // RDD[((Seq[K], Seq[K]), (V, V))]
    val simMeasures: Seq[String] = Seq(
      "ChapmanLengthDeviation", "BlockDistance",
      "ChapmanMeanLength",//ok
      "ChapmanOrderedNameCompoundSimilarity", //character prob
      "CosineSimilarity", "DiceSimilarity", //ok
      "EuclideanDistance", "JaccardSimilarity",
      "Jaro", "JaroWinkler", "Levenshtein", //ok
      "MatchingCoefficient", "MongeElkan",
      "NeedlemanWunch", "OverlapCoefficient",
      "QGramsDistance", "SmithWaterman" ,// ok
      "SmithWatermanGotoh" , //performance prob
      "SmithWatermanGotohWindowedAffine" , //performance prob
      "TagLinkToken", //ok
      "Soundex", "ChapmanMatchingSoundex" //character prob
    )

    val sim1 = simMeasures

    val measure1 = new Measurement(sim1)

    val strategy: Strategy = Strategy(Seq(measure1))

    val featureMatrix = new featureMatrix(sc)
    val features = featureMatrix.matrix(joined, strategy)
    println(features.first())
    println(features.count())
*/
  sc.stop()

  }*/



/*
  def main2(args: Array[String]) {
    //var path = "/Users/jnwang/Research/pdoc/research/mycodes/spark-0.9"
    //val sc = new SparkContext("local", "SimJoin1", path, List("target/scala-2.10/sampleclean_2.10-0.1.jar"))
    val sc = new SparkContext("local", "SimJoin5")
    var sampleData: RDD[(Seq[Int], String)] = sc.parallelize(Seq((Seq(1,1, 2), "A"), (Seq(11,122), "B"), (Seq(1,2,3), "C"), (Seq(1,2,3), "D")))
    var fullData: RDD[(Seq[Int], String)] = sc.parallelize(Seq((Seq(1,2), "X"), (Seq(11,122), "Y"), (Seq(1,2,3), "Z")))

    var sampleDataId = sc.parallelize(Seq((1, (Seq(1,1, 2), "A")), (2, (Seq(11,122), "B")), (3, (Seq(1,2,3), "C")), (4, (Seq(1,2,3), "D"))))
    var simJoin = new SimJoin(sc)
    var joined = simJoin.join(sampleData, fullData, "Jaccard", 0.8)
    joined.foreach(println)

    /*
    sampleDataId.foreach(println)
    var invertedIndex: RDD[(Int, Seq[String])] = sampleDataId.flatMap{
      case (id, (tokenSet, value)) =>
        for (x <- tokenSet)
        yield (x, value)
    }.groupByKey().map(x => (x._1, x._2.distinct))
    invertedIndex.foreach(println)*/


   //simJoin.computeTokenCount(rdd1).foreach(println)

    // Add record ID into sampleData
    //val id = sc.accumulator(0)
    /*val sampleDataID = rdd1.map({ x =>
      id += 1
      (x._1, (id, x._2))
    }).foreach(println)
*/

    // Split SampleData into two separate RDDs: RDD[(Seq[K], ID)] and RDD[(ID, K)]
   // val sampleTokenSet = sampleDataID.map(x => (x._2, x._1))
    //val sampleKalue = sampleDataID.map(x => (x._1, x._3))

   // sampleTokenSet.foreach(println)
   // sampleKalue.foreach(println)

    /*rdd1.flatMap({
      case (tokenSet, value) =>
        for (x <- tokenSet)
        yield (x, value)
    }).groupByKey().map(x => (x._1, x._2.distinct)).foreach(println)*/
  }
*/

//}

//object SimJoin {

  /**
   * Return all pairs of tuples between sampleData and fullData
   * such that overlap(K1, K2)>= threshold.

  def overlapJoin(sampleData: RDD[(K1, K1)], fullData: RDD[(K2, K2)], threshold: Int)
      : RDD[((K1, K1), (K2, K2), simvalue)] = {
    new SimJoin().overlapJoin(sampleData, fullData, threshold)
  }*/



//}