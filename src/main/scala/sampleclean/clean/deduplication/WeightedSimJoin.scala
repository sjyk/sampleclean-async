package sampleclean.clean.deduplication

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Seq
import scala.reflect.ClassTag

/**
 * This class represents a similarity join based on the Jaccard similarity measure.
 * Token global weights are taken into account.
 */
class WeightedJaccardJoin extends WeightedPrefixFiltering{

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  def isSimilar (tokens1: Seq[String],
                 tokens2: Seq[String],
                 threshold: Double,
                 tokenWeights: collection.Map[String, Double]): Boolean = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    if (weight1 < weight2)
      if (weight1 < weight2*threshold) false
    else
      if (weight2 < weight1*threshold) false

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)
    val unionWeight = weight1 + weight2 - intersectionWeight

    if (unionWeight == 0)
      false
    else
      intersectionWeight.toDouble / unionWeight + 1e-6 >= threshold
  }

  /**
   * Calls getRemovedSize method with Jaccard-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    val weight = sumWeight(tokens, tokenWeights)
    super.getRemovedSize(tokens, threshold * weight, tokenWeights)
  }

}

/**
 * This class represents a similarity join based on the overlap between two lists.
 * Token global weights are taken into account.
 */
class WeightedOverlapJoin extends WeightedPrefixFiltering{

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map         
   * @return
   */
  def isSimilar(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): Boolean = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    if (weight1 < weight2)
      if (weight1 < threshold) false
    else
      if (weight2 < threshold) false

      sumWeight(tokens1.intersect(tokens2), tokenWeights) >= threshold
  }

  /**
   * Calls getRemovedSize method with overlap-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    super.getRemovedSize(tokens, threshold, tokenWeights)
  }

}

/**
 * This class represents a similarity join based on the Dice similarity measure.
 * Token global weights are taken into account.
 */
class WeightedDiceJoin extends WeightedPrefixFiltering {

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map         
   */
  def isSimilar(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): Boolean = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    val weightSum = weight1 + weight2
    if (weight1 < weight2)
      if (2*weight1 < weightSum*threshold) false
    else
      if (2*weight2 < weightSum*threshold) false

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSum == 0)
      false
    else
      2 * intersectionWeight.toDouble / weightSum >= threshold

  }

 /**
   * Calls getRemovedSize method with Dice-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    val weight = sumWeight(tokens, tokenWeights)
    super.getRemovedSize(tokens, threshold * weight / (2 - threshold), tokenWeights)
  }


}

/**
 * This class represents a similarity join based on the Cosine similarity measure.
 * Token global weights are taken into account.
 */
class WeightedCosineJoin extends WeightedPrefixFiltering{

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map         
   */
  def isSimilar(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): Boolean = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    val weightSqrt = math.sqrt(weight1 * weight2)
    if (weight1 < weight2)
      if (weight1 < weightSqrt*threshold) false
    else
      if (weight2 < weightSqrt*threshold) false

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSqrt == 0)
      false
    else
      intersectionWeight / weightSqrt >= threshold

  }

  /**
   * Calls getRemovedSize method with Cosine-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    val weight = sumWeight(tokens, tokenWeights)
    super.getRemovedSize(tokens, weight * math.pow(threshold, 2), tokenWeights)
  }

}

/*
class WeightedSimJoin (
                @transient private var sc: SparkContext
                ) extends Serializable {

  def wjoin[K: ClassTag, V: ClassTag](
                                      sampleData: RDD[((Seq[K], V), Long)],
                                      fullData: RDD[((Seq[K], V), Long)],
                                      simfunc: String,
                                      threshold: Double,
                                      broadcast: Boolean = true,
                                      tokenWeights: RDD[(K, Double)])
  : RDD[((Long,Long),(Seq[K], Seq[K]), (V, V))] = {

    simfunc match {
      case "Jaccard" =>
        if (broadcast)
          new WeightedJaccardJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
        else
          new WeightedJaccardJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
      case "Overlap" =>
        if (broadcast)
          new WeightedOverlapJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
        else
          new WeightedOverlapJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
      case "Dice" =>
        if (broadcast)
          new WeightedDiceJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
        else
          new WeightedDiceJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
      case "Cosine" =>
        if (broadcast)
          new WeightedCosineJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)
        else
          new WeightedCosineJoin().broadcastJoin(sc, sampleData, fullData, threshold, tokenWeights)

    }

  }
}

*/

/*
object WeightedSimJoin {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WSimJoin5")
      //.setMaster("spark://Juans-MacBook-Air.local:7077")
      //.setMaster("local[4]")
      //.set("spark.executor.memory", "2g")
      .set("spark.storage.blockManagerSlaveTimeoutMs", "100000")

    val sc = new SparkContext(conf)
/*
    val fullData = args(4) match {

      case "small" => sc.textFile("/Users/juanmanuelsanchez/src/sampleClean/SampleClean/example/data/dblp-small.format", 8).map(x => (x.split(" ").toSeq, 1))
      case "expanded" => sc.textFile("/user/root/expanded", 8).map(x => (x.split(" ").toSeq, 1))
      case "x6" => sc.textFile("/user/root/expandedx6", 16).map(x => (x.split(" ").toSeq, 1))
      case "x12" => sc.textFile("/user/root/expandedx12", 16).map(x => (x.split(" ").toSeq, 1))
    }*/

    val fullData = sc.textFile("/user/root/expanded", 9).map(x => (x.split(" +").toSeq, 1)).zipWithIndex()

    //val testSet = Seq("a b c", "a b d x", "a b d x", "a b d x", "b b a d x", " a b v d x, , ")
    //val fullData = sc.parallelize(testSet).map(x => (x.split(" ").toSeq, 1))

    //val sampleData = fullData.sample(false, args(0).toDouble, 6)
    val sampleData = fullData.sample(false, 1, 6)
    //val sampleData = sc.parallelize(Seq( "a b d x c", "b c")).map(x => (x.split(" ").toSeq, 1))


    //val tokenWeights: RDD[(String, Double)] = IDFW.openIDFMap("/Users/juanmanuelsanchez/Documents/IDFMap")






    val joined = {
      if(args(2) == "true") {
        // Include global IDF weights
        val IDFW = IDFWeights(fullData.map(_._1))
        val tokenWeights: RDD[(String, Double)] = {
          if (args(3) == "true") {
            val IDFmap = IDFW.getIDFMap
            sc.makeRDD(IDFmap.asInstanceOf[collection.Map[String, Double]].toSeq)
          }
          else IDFW.openIDFMap(sc, "/IDFMap" + args(4))
        }
        //println("tokenWeights count: " + tokenWeights.count)
        val simJoin = new WeightedSimJoin(sc)
        simJoin.wjoin(sampleData, fullData, "Jaccard", args(1).toDouble, tokenWeights = tokenWeights)
      }
      else {
        val IDFW = IDFWeights(fullData.map(_._1))
        val tokenWeights: RDD[(String, Double)] = {
          val IDFmap = IDFW.getUnweightedMap
          sc.makeRDD(IDFmap.asInstanceOf[collection.Map[String, Double]].toSeq)
        }

        val simJoin = new WeightedSimJoin(sc)
        simJoin.wjoin(sampleData, fullData, "Jaccard", args(1).toDouble, tokenWeights = tokenWeights)
      }

    }.cache().setName("joined")

    println("joined count: " + joined.count())
    println("joined first: " + joined.first)

    //testing
    val test_join = sc.textFile("/user/root/WtestExpanded").map(x => x.split(" ").toSeq).map(x => ((x(1).toLong, x(2).toLong), 1))
    //val test_join = sc.textFile("/Users/juanmanuelsanchez/Documents/sampleCleanData/jaccard_0.8_clean").map(x => ((x(1).toLong, x(2).toLong), 1))
    val result = joined.map(x => (x._1._2, x._1._1))



    var fstream = new FileWriter("/testScript/wcontrol.txt", false)
    var out = new BufferedWriter(fstream)
    for (pair <- test_join.collect.toSeq) yield {
      out.write(pair.toString())
      out.newLine()
    }
    out.close()

    fstream = new FileWriter("/testScript/wresult.txt", false)
    out = new BufferedWriter(fstream)
    for (pair <- result.collect.toSeq) yield {
      out.write(pair.toString())
      out.newLine()
    }
    out.close()


    /*

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
  }


}

*/