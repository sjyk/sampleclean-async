package sampleclean.clean.featurize

import org.apache.spark.sql.Row
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import uk.ac.shef.wit.simmetrics.similaritymetrics._
import uk.ac.shef.wit.simmetrics.tokenisers.TokeniserWhitespace

/**
 * One common type of featurizers are Similarity Featurizers;
 * these are widely used in deduplication and entity resolution workflows.
 * In this case, a Similarity Featurizer will take two rows and calculate a series
 * of values that represent different aspects of "similiarity" between
 * them.
 * 
 * Each aspect will be one of the following similarity metrics:
 *
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

  /**
   * This function takes a set of rows and returns a tuple.
   *
   * The first element of the tuple is a set of primary keys and the second
   * element is an array of doubles which are the features (known as a feature vector).
   *
   * @param rows A set of Rows. If more than 2, only the first and last will be compared.
   * @param params not needed for this implementation.
   */
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

  /**
   * This function calculates a list of similarities between two strings.
   * All use white space tokenizer by default and assume equal token weights
   *
   * @param s1 First string
   * @param s2 Second string
   * @param simMeasures list of allowed similarity measures (see [[SimilarityFeaturizer]] )
   */
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
            case "JaccardSimilarity" => new WeightedJaccardSimilarity(List(), List(), null)
            case "DiceSimilarity" => new WeightedDiceSimilarity(List(), List(), null)
            case "CosineSimilarity" => new WeightedCosineSimilarity(List(), List(), null)
            case "OverlapSimilarity" => new WeightedOverlapSimilarity(List(), List(), null)
            case "EditDistance" => new EditBlocking(List(), List(), null)

        		case _ => throw new NoSuchElementException(measure + " measure not found")
      		}
    		)

      def omitSpecialCharacters(m:AbstractStringMetric, s1:String, s2:String):Double = {
        val US_EN_MAP: Array[Char] = "01230120022455012623010202".toCharArray
        val trimmed1 = s1.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
        val trimmed2 = s2.filter(x => (x.toUpper - 'A') < US_EN_MAP.length)
        m.asInstanceOf[AbstractStringMetric].getSimilarity(trimmed1, trimmed2).toDouble
      }

    		measures.map(measure =>
          measure match {
            // Fix for similarity measures that have issues with special characters
            case m: Soundex => omitSpecialCharacters(m,s1,s2)
            case m: ChapmanMatchingSoundex => omitSpecialCharacters(m,s1,s2)
            case m: ChapmanOrderedNameCompoundSimilarity => omitSpecialCharacters(m,s1,s2)

              // SampleClean implementations
            case m: AnnotatedSimilarityFeaturizer => {
              val tokenizer = new TokeniserWhitespace()
              val tokens1 = tokenizer.tokenizeToArrayList(s1).toArray.toSeq.asInstanceOf[Seq[String]]
              val tokens2 = tokenizer.tokenizeToArrayList(s2).toArray.toSeq.asInstanceOf[Seq[String]]

              m.similarity(tokens1,tokens2,Map[String,Double]())

            }
            case _ => measure.asInstanceOf[AbstractStringMetric].getSimilarity(s1, s2).toDouble
          }

    		)
  		}

}