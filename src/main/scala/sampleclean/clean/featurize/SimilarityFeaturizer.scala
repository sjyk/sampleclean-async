package sampleclean.clean.featurize

import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._

import uk.ac.shef.wit.simmetrics.similaritymetrics._
import uk.ac.shef.wit.simmetrics.tokenisers.TokeniserWhitespace

/**
 * One common type of featurizers are similarity featurizers which
 * are widely used in deduplication and entity resolution workflows.
 * 
 * This set of featurizers takes in a list of metrics. Acceptable ones
 * include:
"BlockDistance", "ChapmanLengthDeviation", "ChapmanMatchingSoundex"
"ChapmanMeanLength", "ChapmanOrderedNameCompoundSimilarity", "CosineSimilarity"
"DiceSimilarity", "EuclideanDistance", "JaccardSimilarity", "Jaro"
"JaroWinkler" ,"Levenshtein", "MatchingCoefficient","MongeElkan"
"NeedlemanWunch", "OverlapCoefficient", "QGramsDistance", "SmithWaterman"
"SmithWatermanGotoh", "SmithWatermanGotohWindowedAffine", "Soundex"
"TagLinkToken"
 */
@serializable
class SimilarityFeaturizer(colNames: List[String], 
                           context:List[String], 
                           metrics:List[String]) 
	extends Featurizer(colNames, context){

		def featurize[K,V](rows: Set[Row], params: collection.immutable.Map[K,V]=null): (Set[Row], Array[Double]) = {

			val rowA = rows.head
			val rowB = rows.last

			var stringA = ""
			var stringB = ""
			for (col <- cols){
				stringA = stringA + " " +rowA(col)
				stringB = stringB + " " +rowB(col)
			}

			return (Set(rowA, rowB),
				    getSimilarities(stringA,stringB,metrics).toArray
				   )
		}

		def getSimilarities(s1: String, s2: String, simMeasures: List[String] = metrics): List[Double] = {

    		val measures: List[Object] = simMeasures.map(measure =>
      		measure match {
        		case "BlockDistance" => new BlockDistance
        		case "ChapmanLengthDeviation" => new ChapmanLengthDeviation
        		case "ChapmanMatchingSoundex" => new ChapmanMatchingSoundex
        		case "ChapmanMeanLength" => new ChapmanMeanLength
        		case "ChapmanOrderedNameCompoundSimilarity" => new ChapmanOrderedNameCompoundSimilarity
            case "EuclideanDistance" => new EuclideanDistance
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

            // SampleClean implementations
            case "JaccardSimilarity" => new WeightedJaccardSimilarity(List(""), List(""), null,0)
            case "DiceSimilarity" => new WeightedDiceSimilarity(List(""), List(""), null,0)
            case "CosineSimilarity" => new WeightedCosineSimilarity(List(""), List(""), null,0)
            case "OverlapSimilarity" => new WeightedOverlapSimilarity(List(""), List(""), null,0)
            case "EditDistance" => new EditBlocking(List(""), List(""), null,0)

        		case _ => throw new NoSuchElementException(measure + " measure not found")
      		}
    		)


    		measures.map(measure =>
          measure match {
            // Fix for similarity measures that have issues with special characters
            case m @ (Soundex | ChapmanMatchingSoundex | ChapmanOrderedNameCompoundSimilarity) => {
              // functions implemented only support US_EN alphabet; non-valid characters are omitted
              val US_EN_MAP: Array[Char] = "01230120022455012623010202".toCharArray
              val trimmed1 = s1.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
              val trimmed2 = s2.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
              m.asInstanceOf[AbstractStringMetric].getSimilarity(trimmed1, trimmed2).toDouble
            }
              // SampleClean implementations
            case m: AnnotatedSimilarityFeaturizer => {
              val tokenizer = new TokeniserWhitespace()
              val tokens1 = tokenizer.tokenizeToArrayList(s1).toArray.toSeq.asInstanceOf[Seq[String]]
              val tokens2 = tokenizer.tokenizeToArrayList(s2).toArray.toSeq.asInstanceOf[Seq[String]]

              m.getSimilarity(tokens1,tokens2,Map[String,Double]())

            }
            case _ => measure.asInstanceOf[AbstractStringMetric].getSimilarity(s1, s2).toDouble
          }

    		)
  		}

}