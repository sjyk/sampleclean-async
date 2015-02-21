package sampleclean.clean.deduplication.blocker

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

abstract class Blocker(scc: SampleCleanContext,
              		   sampleTableName:String) extends Serializable {

	def block(input:RDD[Row]):RDD[Set[Row]]

}


