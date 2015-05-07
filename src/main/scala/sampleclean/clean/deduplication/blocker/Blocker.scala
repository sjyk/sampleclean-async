package sampleclean.clean.deduplication.blocker

import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * This class is used to construct blocker objects. A blocker is
 * an algorithm that separates a dataset into blocks.
 *
 * @param scc SampleClean Context
 * @param sampleTableName
 */
abstract class Blocker(scc: SampleCleanContext,
              		   sampleTableName:String) extends Serializable {

	def block(input:RDD[Row]):RDD[Set[Row]]

	def updateContext(newContext:List[String])
}


