package sampleclean.clean.algorithm

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

@serializable
/**
 * The abstract SampleCleanAlgorithm defines the super class of
 *  all algorithms for data cleaning. Every algorithm is defined
 *  on a sample of data.
*/
abstract class SampleCleanAlgorithm(params:AlgorithmParameters, 
									scc: SampleCleanContext, 
									var sampleTableName: String) {

	/**Defines the pipeline with which this algorithm is associated
	 */
	private [sampleclean] var pipeline:SampleCleanPipeline = null

	/**Gives a logical name to this algorithms
	*/
  private [sampleclean] var name:String = null

	/**Execute this algorithm synchronously or asychronously
	*/
  private [sampleclean] var blocking:Boolean = true

	/**The execution function of this algorithm
	*/
	def exec()

	/**
	 * This function is called by the algorithm designer to notify the 
	 * pipeline that the model has been updated.
	 */
	def onUpdateNotify()={

		if(pipeline != null)
			pipeline.notification()

	}

  private [sampleclean] def setSampleName(newSampleName:String) = {
		sampleTableName = newSampleName
	}

	def synchronousExecAndRead():SchemaRDD = {
		exec()
		return scc.getCleanSample(sampleTableName)
	}

}