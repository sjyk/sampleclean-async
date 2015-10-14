package sampleclean.util

import sampleclean.api.SampleCleanContext
import sampleclean.api.WorkingSet

/**
 * A loader is the abstract class that handles the I/O between a file and 
 * the persistent store.
 * @type {[type]}
 */
abstract class Loader(scc:SampleCleanContext,
					  schema:List[(String,String)])
{
	/**
	 * This performs given the input file returns a 
	 * reference to the base table.
	 */
	def load2Hive(tableName:String=""):String

	/**
	 * This turns the loaded base table into a reference to the
	 * working set.
	 * @type {[type]}
	 */
	def load(samplingRatio:Double=1.0,namedSet:String=""):WorkingSet = {
		val baseTable = if (namedSet == "") load2Hive() else load2Hive(namedSet)
		var workingSetName = baseTable + "_sample"
		scc.initialize(baseTable,workingSetName,samplingRatio)
		return new WorkingSet(scc,workingSetName)
	}
}