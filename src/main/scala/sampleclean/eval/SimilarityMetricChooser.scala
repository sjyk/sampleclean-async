package sampleclean.eval
import sampleclean.clean.deduplication.join._
import sampleclean.clean.deduplication.blocker._
import sampleclean.clean.deduplication.matcher._
import sampleclean.clean.deduplication._
import sampleclean.api.SampleCleanContext
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import sampleclean.clean.featurize.Tokenizer._


class SimilarityMetricChooser(scc: SampleCleanContext,
							  eval:Evaluator) {

	def tuneThresholdAndMetric(sampleTableName: String, 
							   cols:List[String]):AnnotatedSimilarityFeaturizer = {

	val metricList = Array( new WeightedJaccardSimilarity(cols, 
                                                   scc.getTableContext(sampleTableName),
                                                   WordTokenizer(), 
                                                   0.0) ,

							new WeightedOverlapSimilarity(cols, 
                                                   scc.getTableContext(sampleTableName),
                                                   WordTokenizer(), 
                                                   0.0) ,

							new WeightedDiceSimilarity(cols, 
                                                   scc.getTableContext(sampleTableName),
                                                   WordTokenizer(), 
                                                   0.0) ,

							new WeightedCosineSimilarity(cols, 
                                                   scc.getTableContext(sampleTableName),
                                                   WordTokenizer(), 
                                                   0.0))

	var minMetric:AnnotatedSimilarityFeaturizer = null
	var candidateCountMin:Long = -1
	for(metric <- metricList){
			var m = new MonotonicSimilarityThresholdTuner(scc,eval,metric)
			var threshold = m.tuneThreshold(sampleTableName)
			var candidateCount = m.getCandidatePairsCount(sampleTableName, threshold)	
			if(candidateCountMin == -1 || candidateCount < candidateCountMin)
			{
				metric.threshold = threshold
				minMetric = metric
				candidateCountMin = candidateCount
				println(metric + " " + candidateCount)
			}	
	}

	return minMetric

	}
}