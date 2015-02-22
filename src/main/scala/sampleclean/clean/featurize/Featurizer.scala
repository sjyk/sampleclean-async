package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}

/* The featurizer Abstract Class takes a set of records
 * and returns a list of numbers in R^d
 *
 * We define featurization to be symmetric where the features of
 * {a, b} == {b, a}
 */
@serializable
abstract class Featurizer(colNames: List[String],contextIn:List[String]){

	var cols:List[Int] = List()
	var context = contextIn
	setContext(context)

	/**
	 * This function takes a set of rows, takes a set of parameters that 
	 * are specified in a Map and returns a tuple.
	 *
	 * The first element of the tuple is a set of primary keys and the second
	 * element is an array of doubles which are the features.
	 * 
	 * @type {[type]}
	 */
	def featurize[K,V](rows: Set[Row], params: collection.immutable.Map[K,V]=null): (Set[Row], Array[Double])


	def setContext(contextUp:List[String]) = {

		context = contextUp
		var newCols:List[Int] = List()
		for(col <- colNames)
			if (context.indexOf(col) >= 0)
				newCols = context.indexOf(col) :: newCols
			else
				println("Dropping col: " + col + " because it does not exist in the current context")

		cols = newCols.reverse
	}

}