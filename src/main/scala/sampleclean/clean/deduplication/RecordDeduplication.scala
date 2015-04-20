package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import sampleclean.clean.deduplication.join.BlockerMatcherJoinSequence


/**
 * The Abstract Deduplication class builds the structure for
 * subclasses that implement deduplication. It has two basic
 * primitives a blocking function and then an apply function (implemented)
 * in subclasses.
 */
class RecordDeduplication(params:AlgorithmParameters, 
							scc: SampleCleanContext,
							sampleTableName: String,
							components: BlockerMatcherJoinSequence) extends
							SampleCleanAlgorithm(params, scc, sampleTableName) {

		//fix
		val colList = (0 until scc.getHiveTableSchema(scc.qb.getCleanSampleName(sampleTableName)).length).toList
		
		//these are dynamic class variables
    	var hashCol = 0

    	/*
      	Sets the dynamic variables at exec time
     	*/
    	def setTableParameters(sampleTableName: String) = {
        	hashCol = scc.getColAsIndex(sampleTableName,"hash")
    	}

		def exec()={
			val sampleTableRDD = scc.getCleanSample(sampleTableName).repartition(scc.getSparkContext().defaultParallelism)
			val fullTableRDD = scc.getFullTable(sampleTableName).repartition(scc.getSparkContext().defaultParallelism)
			apply(components.blockAndMatch(sampleTableRDD,fullTableRDD))
		}

		def apply(filteredPairs:RDD[(Row,Row)]) = {
			 val dupCounts = filteredPairs.map{case (fullRow, sampleRow) =>
        			(sampleRow(hashCol).asInstanceOf[String],1)} // SHOULD unify hash and idCol
        			.reduceByKey(_ + _)
        			.map(x => (x._1,x._2+1)) // Add back the pairs that are removed above
        	 scc.updateTableDuplicateCounts(sampleTableName, dupCounts)
		}

}

object RecordDeduplication{
  
}