package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.clean.deduplication.join.{BroadcastJoin, BlockerMatcherJoinSequence}
import sampleclean.clean.deduplication.matcher.AllMatcher
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.WordTokenizer


/**
 * This is an abstract class for record deduplication. It
 * implements a basic structure and error handling for the class.
 * @param scc SampleClean context
 * @param sampleTableName
 * @param components blocker + matcher routine.
 */
class RecordDeduplication(scc: SampleCleanContext,
							sampleTableName: String,
							val components: BlockerMatcherJoinSequence) extends
							SampleCleanAlgorithm(null, scc, sampleTableName) {

		//fix
		private [sampleclean] val colList = (0 until scc.getHiveTableSchema(scc.qb.getCleanSampleName(sampleTableName)).length).toList
		
		//these are dynamic class variables
    private [sampleclean] var hashCol = 0

    // Sets the dynamic variables at exec time
    private [sampleclean] def setTableParameters(sampleTableName: String) = {
        hashCol = scc.getColAsIndex(sampleTableName,"hash")
    }

  /**
   * Execute Record Deduplication
   */
		def exec()={
			val sampleTableRDD = scc.getCleanSample(sampleTableName).repartition(scc.getSparkContext().defaultParallelism)
			val fullTableRDD = scc.getFullTable(sampleTableName).repartition(scc.getSparkContext().defaultParallelism)
			apply(components.blockAndMatch(sampleTableRDD,fullTableRDD))
		}

		private [sampleclean] def apply(filteredPairs:RDD[(Row,Row)]) = {
			 val dupCounts = filteredPairs.map{case (fullRow, sampleRow) =>
        			(sampleRow(hashCol).asInstanceOf[String],1.0)} // SHOULD unify hash and idCol
        			.reduceByKey(_ + _)
        			.map(x => (x._1,x._2+1.0)) // Add back the pairs that are removed above
        	 scc.updateTableDuplicateCounts(sampleTableName, dupCounts)
		}

}

object RecordDeduplication{

  /**
   * This method builds an Record Deduplication algorithm that will
   * resolve automatically. It uses several default values and is designed
   * for simple deduplication tasks. For more flexibility in
   * parameters (such as setting a Similarity Featurizer and Tokenizer),
   * refer to the [[RecordDeduplication]] class.
   *
   * This algorithm uses the Jaccard Similarity for pairwise comparisons
   * and a word tokenizer.
   *
   * @param scc SampleClean Context
   * @param sampleName
   * @param colNames names of attributes that will be used for deduplication
   * @param threshold threshold used in the algorithm. Must be
   *                  between 0.0 and 1.0
   * @param weighting If set to true, the algorithm will automatically calculate
   *                 token weights. Default token weights are defined based on
   *                 token idf values.
   *
   *                 Adding weights into the join might lead to more reliable
   *                 pair comparisons and speed up the algorithm if there is
   *                 an abundance of common words in the dataset.
   */
  def deduplication(scc:SampleCleanContext,
                             sampleName:String,
                             colNames: List[String],
                             threshold:Double=0.9,
                             weighting:Boolean =true):RecordDeduplication = {

    val similarity = new WeightedJaccardSimilarity(colNames,
                                  scc.getTableContext(sampleName),
                                  WordTokenizer(),
                                  threshold)

    val join = new BroadcastJoin(scc.getSparkContext(), similarity, weighting)
    val matcher = new AllMatcher(scc, sampleName)

    val blockerMatcher = new BlockerMatcherJoinSequence(scc,sampleName, join, List(matcher))
    return new RecordDeduplication(scc,sampleName,blockerMatcher)

  }
}