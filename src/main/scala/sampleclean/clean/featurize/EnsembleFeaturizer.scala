package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}

/* With the ensemble featurizer we can take a set of featurizers and 
combine them together.
 */
@serializable
abstract class EnsembleFeaturizer(cols: List[Int], featurizers:List[Featurizer]){

	/**
	 */
	def featurize[K,V](rows: Set[Row], params: collection.immutable.Map[K,V]=null): (Set[Row], Array[Double])=
	{
		var pkset:Set[Row] = Set()
		var feature:Array[Double] = Array()
		for (featurizer <- featurizers)
		{
			val result = featurizer.featurize(rows, params)
			pkset = pkset ++ result._1
			feature = Array.concat(feature, result._2)
		}

		return (pkset, feature)
	}

}