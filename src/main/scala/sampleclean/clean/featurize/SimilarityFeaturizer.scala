package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}
import uk.ac.shef.wit.simmetrics.similaritymetrics._

/* This class implements the similarity based featurizer used in Deduplication
 */
@serializable
class SimilarityFeaturizer(cols: List[Int], metrics:List[String]) 
	extends Featurizer(cols){

		def featurize(rows: Set[Row], params: Map[Any,Any]=null): (Set[String], Array[Double]) = {

			val rowA = rows.head
			val rowB = rows.last

			var stringA = ""
			var stringB = ""
			for (col <- cols){
				stringA = stringA + " " +rowA(col).asInstanceOf[String]
				stringB = stringB + " " +rowB(col).asInstanceOf[String]
			}

			return (Set(rowA(1).asInstanceOf[String],
									rowB(1).asInstanceOf[String]),
				    getSimilarities(stringA,stringB,metrics).toArray
				   )
		}

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