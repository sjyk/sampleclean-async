package sampleclean.clean.deduplication

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import uk.ac.shef.wit.simmetrics.similaritymetrics._


/**
 * Class used to create a valid strategy. A valid strategy may imply
 * one or more feature values, depending on the number of chosen metrics to be used.
 * @param colNames columns to be concatenated.
 * @param simMeasures list of valid names of similarity measures as per simmetrics library
 */


case class Feature (colNames: List[String], simMeasures: List[String]) extends Serializable



/**
 * This class defines the Feature Vector of a pair of records.
 * @param features list of valid strategies to be combined into the vector.
 * @param lowerCase if true, converts all characters to lower case.
 */
case class FeatureVector (features: List[Feature],
                          colMapper1: List[String] => List[Int],
                          colMapper2: List[String] => List[Int],
                          lowerCase: Boolean = true){


  // Transforms pairs of records into feature vectors.
  def toFeatureVectors(rowPairs: RDD[(Row, Row)]): RDD[Array[Double]] = {
    rowPairs.map(x => toFeatureVector(x._1, x._2))
  }

  // Transforms a pair of records into a feature vector.
  def toFeatureVector(rowPair: (Row, Row)): Array[Double] = {
    toFeatureVector(rowPair._1, rowPair._2)
  }

  // Returns chosen metrics for two records
  def toFeatureVector(row1: Row, row2: Row): Array[Double] = {

    features.map {
      case feature: Feature => {
        val cols1 = colMapper1(feature.colNames)
        val cols2 = colMapper2(feature.colNames)
        val simMeasures = feature.simMeasures

        var concatenateCols1 = cols1.foldLeft("")((result, current) =>
          result + " " + (if (row1.isNullAt(current) || row1.isDefinedAt(current)) row1(current).toString else ""))
        var concatenateCols2 = cols2.foldLeft("")((result, current) =>
          result + " " + (if (row2.isNullAt(current) || row2.isDefinedAt(current)) row2(current).toString else ""))
        if (lowerCase) {
          concatenateCols1 = concatenateCols1.toLowerCase()
          concatenateCols2 = concatenateCols2.toLowerCase()
        }
        getSimilarities(concatenateCols1, concatenateCols2, simMeasures)
      }
    }.flatten.toArray
  }

  /**
   * Returns a list of similarity measures for two strings.
   * @param s1 first string
   * @param s2 second string
   * @param simMeasures list of valid similarity measures. Names are based on simmetrics library.
   */
  def getSimilarities(s1: String, s2: String, simMeasures: List[String]): List[Double] = {
    val measures: List[Object] = simMeasures.map(measure =>
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

    // Fix for similarity measures that have issues with special characters
    measures.map(measure => {
      if (measure.isInstanceOf[Soundex] || measure.isInstanceOf[ChapmanMatchingSoundex] || measure.isInstanceOf[ChapmanOrderedNameCompoundSimilarity]){
        // functions implemented only support US_EN alphabet; non-valid characters are omitted
        val US_EN_MAP: Array[Char] = "01230120022455012623010202".toCharArray
        val trimmed1 = s1.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
        val trimmed2 = s2.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
        measure.asInstanceOf[AbstractStringMetric].getSimilarity(trimmed1, trimmed2).toDouble
      }
      else
        measure.asInstanceOf[AbstractStringMetric].getSimilarity(s1, s2).toDouble
    })
  }

}




