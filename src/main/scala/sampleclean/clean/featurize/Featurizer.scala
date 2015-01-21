package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}

/* The featurizer Abstract Class takes a set of records
 * and returns a list of numbers in R^d
 *
 * We define featurization to be symmetric where the features of
 * {a, b} == {b, a}
 */
@serializable
abstract class Featurizer(){

	/**
	 * This function takes a set of rows, takes a set of parameters that 
	 * are specified in a Map and returns a tuple.
	 *
	 * The first element of the tuple is a set of primary keys and the second
	 * element is a list of doubles which are the features.
	 * 
	 * @type {[type]}
	 */
	def featurize(rows: Set[Row], params: Map[Any,Any]): (Set[String], List[Double])

}