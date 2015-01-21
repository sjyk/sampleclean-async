package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}

/* 
 */
@serializable
abstract class EnsembleFeaturizer(cols: List[Int], featurizers:List[Featurizer]){

	/**
	 */
	def featurize(rows: Set[Row], params: Map[Any,Any]=null): (Set[String], Array[Double])=
	{
		var pkset:Set[String] = Set()
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