package sampleclean.clean

import org.apache.spark.sql.SchemaRDD

@serializable
abstract class SampleCleanAlgorithm() {

	def exec(input: SchemaRDD) : String

}