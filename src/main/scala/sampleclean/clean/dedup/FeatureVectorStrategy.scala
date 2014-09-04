package sampleclean.clean.dedup

import org.apache.spark.rdd.RDD
import scala.collection.Seq
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.{SparkConf, SparkContext}
import uk.ac.shef.wit.simmetrics.similaritymetrics._


// Class used to create a valid strategy.
class Feature (cols1: Seq[Int],
                 cols2: Seq[Int],
                 simMeasures: Seq[String]) extends Serializable {

  def getColsPair: (Seq[Int],Seq[Int]) = {(cols1, cols2)}
  def getMeasures: Seq[String] = { simMeasures }

  // After we figure out a simple way to iterate over the attributes of a row, we can uncomment the following codes.

  /*def this(sim: Seq[String]) = {
      this(null, null, sim)
  }*/
}

case class FeatureVectorStrategy (features: Seq[Feature], lowerCase: Boolean = true){

  def toFeatureVectors(recordPairs: RDD[(Row, Row)]): RDD[Seq[Float]] = {
    recordPairs.map(x => toFeatureVector(x._1, x._2))
  }

  // Returns chosen metrics for two records
  def toFeatureVector(row1: Row, row2: Row): Seq[Float] = {

    features.map {
      case feature: Feature => {
        val (cols1, cols2) = feature.getColsPair
        val simMeasures = feature.getMeasures
        // if there are no indices as input, assume all indices will be considered

        // I commented this two lines since I haven't figured out how to get the length of a row
        //val indicesRecord1 = if (pair._1 == null) Range(0,vec1.length) else pair._1
        //val indicesRecord2 = if (pair._2 == null) Range(0,vec2.length) else pair._2
        // If index is out of range, assume string = ""; this could change
        var concatenateCols1 = cols1.foldLeft("")((result, current) => result + " " + (if (row1.isDefinedAt(current)) row1(current).toString else ""))
        var concatenateCols2 = cols2.foldLeft("")((result, current) => result + " " + (if (row2.isDefinedAt(current)) row2(current).toString else ""))
        if (lowerCase) {
          concatenateCols1 = concatenateCols1.toLowerCase()
          concatenateCols2 = concatenateCols2.toLowerCase()
        }
        getSimilarities(concatenateCols1, concatenateCols2, simMeasures)
      }
    }.flatten
  }

  def getSimilarities(s1: String, s2: String, simMeasures: Seq[String]): Seq[Float] = {
    val measures: Seq[Object] = simMeasures.map(measure =>
      measure match {
        case "BlockDistance" => new BlockDistance
        case "ChapmanLengthDeviation" => new ChapmanLengthDeviation
        case "ChapmanMatchingSoundex" => new ChapmanMatchingSoundex
        case "ChapmanMeanLength" => new ChapmanMeanLength
        case "ChapmanOrderedNameCompoundSimilarity" => new ChapmanOrderedNameCompoundSimilarity
        case "CosineSimilarity" => new CosineSimilarity
        case "DiceSimilarity" => new DiceSimilarity
        case "EuclideanDistance" => new EuclideanDistance
        case "JaccardSimilarity" => new JaccardSimilarity
        case "Jaro" => new Jaro
        case "JaroWinkler" => new JaroWinkler
        case "Levenshtein" => new Levenshtein
        case "MatchingCoefficient" => new MatchingCoefficient
        case "MongeElkan" => new MongeElkan
        case "NeedlemanWunch" => new NeedlemanWunch
        case "OverlapCoefficient" => new OverlapCoefficient
        case "QGramsDistance" => new QGramsDistance
        case "SmithWaterman" => new SmithWaterman
        case "SmithWatermanGotoh" => new SmithWatermanGotoh
        case "SmithWatermanGotohWindowedAffine" => new SmithWatermanGotohWindowedAffine
        case "Soundex" => new Soundex
        case "TagLinkToken" => new TagLinkToken
        case _ => throw new NoSuchElementException(measure + " measure not found")
      }
    )

    measures.map(measure => {
      if (measure.isInstanceOf[Soundex] || measure.isInstanceOf[ChapmanMatchingSoundex] || measure.isInstanceOf[ChapmanOrderedNameCompoundSimilarity]){
        // functions implemented only support US_EN alphabet; non-valid characters are omitted
        val US_EN_MAP: Array[Char] = "01230120022455012623010202".toCharArray
        val trimmed1 = s1.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
        val trimmed2 = s2.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
        measure.asInstanceOf[AbstractStringMetric].getSimilarity(trimmed1, trimmed2)
      }
      else
        measure.asInstanceOf[AbstractStringMetric].getSimilarity(s1, s2)
    })
  }

}

/*
object FeatureVectorStrategy {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("FeatureMatrix")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    // RDD[((Seq[K], Seq[K]), (V, V))] output of SimJoin
    val joined: RDD[((Seq[String], Seq[String]), (Int, Int))] = sc.textFile("/Users/juanmanuelsanchez/Documents/joined_clean.txt", 8).map(x => {

      val seqs = x.split(":::")(0).split("::")
      ((seqs(0).split(",").toSeq, seqs(1).split(",").toSeq), (1,1))

    })


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
    val sim1 = Seq(simMeasures(20),simMeasures(1))
    val sim2 = Seq(simMeasures(1), simMeasures(2))
    val sim3 = Seq(simMeasures(3))



    val vec1 = Seq("Í", "a c b", "c c c", "abc")
    val vec2 = Seq("a b c", "a b c", "cc c", "abc ab1")
    val vec3 = Seq("aaa", "ssa aa", "bbb", "abaa ab c")



    // Data is RDD[((Seq[K], Seq[K]), (V, V))]
    val pair1: ((Seq[String], Seq[String]), (Int, Int)) = ((vec1, vec2), (1,1))
    val pair2 = ((vec2, vec3), (1,1))
    val pair3 = ((vec3, vec1), (1,1))
    val data: RDD[((Seq[String], Seq[String]), (Int, Int))] = sc.parallelize(Seq(pair1, pair2, pair3))

    val m1 = new Measurement(Seq(1,2), Seq(1), sim1)
    val m2 = new Measurement(Seq(1),Seq(1,2), sim2)
    val m3 = new Measurement(Seq(3),Seq(2,3,0), sim3)
    val m4 = new Measurement(sim1)

    val fvStrategy = FeatureVectorStrategy(Seq(m4))
    val matrix = fvStrategy.toFeatureVectors(joined)
    println(joined.first())
    println(matrix.first())
    println(matrix.count())

    sc.stop()

  }

}*/



