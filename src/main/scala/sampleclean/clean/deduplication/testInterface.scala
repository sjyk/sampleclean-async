package sampleclean.clean.deduplication

import org.apache.spark.{sql, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.Seq

/**
 * Created by juanmanuelsanchez on 12/16/14.
 */

trait SCSimilarityFunction extends SimilarityFunction {

  val key1: BlockingKey
  val key2: BlockingKey
  val minimumTokens: Int

  def broadcastJoin2 (sc: SparkContext,
                     threshold: Double,
                     fullTable: RDD[Row],
                     fullKey: BlockingKey,
                     sampleTable: RDD[Row],
                     sampleKey: BlockingKey,
                     minSize:Int): RDD[(Row,Row,Double)]

}

//user can create their own similarity function
//attr includes necessary parameters to calculate similarity
class SimilarityFunction(name: String, f: (Row,Row, List[Any]) => Double) {

  def calculateSimilarity(row1: Row,row2: Row, attr: List[Any] = attr): Double

}

// attr:
//       (0) (blockingKey1, blockingKey2)
//       (1) minSize
// weights are automatically calculated
class wJaccardSim(keys: (BlockingKey,BlockingKey),
                  minSize:Int) extends SCSimilarityFunction {

  val key1 = keys._1
  val key2 = keys._2
  val minimumTokens = minSize

  val attr = List(keys,minSize)
  val SCFunction = new WeightedJaccardJoin
  val name = "WeightedJaccard"

  def calculateSimilarity(row1: Row,row2: Row, attr: List[Any] = attr) = {
    SCFunction.calculateSimilarity(row1: Row,row2: Row, attr)
  }


  def broadcastJoin2 (sc: SparkContext,
                     threshold: Double,
                     fullTable: RDD[Row],
                     fullKey: BlockingKey,
                     sampleTable: RDD[Row],
                     sampleKey: BlockingKey,
                     minSize:Int): RDD[(Row,Row,Double)] = {
    SCFunction.broadcastJoin2(sc,threshold,fullTable,fullKey,sampleTable,sampleKey,minSize)
  }


}


object SimilarityFunctionLibrary {
  val defaultKey = BlockingKey(Seq(0),WhiteSpaceTokenizer())
  val wJaccardSimilarity: SimilarityFunction = new wJaccardSim((defaultKey,defaultKey),0)
}


class testInterface {
  def createSimRDD(sc: SparkContext,
                   rdd1: RDD[Row],
                   rdd2: RDD[Row],
                   similarityFunction: SimilarityFunction,
                   threshold: Double): RDD[(Row, Row, Double)] = {

    val simRDD = similarityFunction.name match {
      case "WeightedJaccard" => {

        val SCSimFunc = similarityFunction
                        .asInstanceOf[SCSimilarityFunction]

         SCSimFunc.broadcastJoin2(sc, threshold,
                                 rdd1, SCSimFunc.key1,
                                 rdd2, SCSimFunc.key2,
                                 SCSimFunc.minimumTokens)
      }

      case _ => rdd1.cartesian(rdd2)
                .map(x => (x._1, x._2, similarityFunction.calculateSimilarity(x._1, x._2)))
                .filter(_._3 <= threshold)
    }

    simRDD

  }

  case class SimilarityFeaturizedPair(row1:Row, row2:Row, featureVector:Array[Double])

  def featurizeSimRDD( simRDD:RDD[(Row, Row, Double)],
                       featureAttrList: List[(List[Int], SimilarityFunction)]
                       ): RDD[SimilarityFeaturizedPair] = {

    val features = featureAttrList.map(x => Feature2(x._1, List(x._2.name)))

    val featureVector = FeatureVector2(features)

    simRDD.map(x => {
      val vector = featureVector.toFeatureVector(x._1,x._2)
      SimilarityFeaturizedPair(x._1,x._2,vector)
    })

  }

  def classify(featurizedRDD:RDD[SimilarityFeaturizedPair],
               model: SimilarityFeaturizedPair => Boolean): RDD[SimilarityFeaturizedPair] = {

    featurizedRDD.filter(model)
  }

}




