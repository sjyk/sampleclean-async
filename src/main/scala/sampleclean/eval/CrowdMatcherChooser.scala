package sampleclean.eval
import sampleclean.clean.deduplication.join._
import sampleclean.clean.deduplication.blocker._
import sampleclean.clean.deduplication.matcher._
import sampleclean.clean.deduplication._
import sampleclean.api.SampleCleanContext
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sampleclean.clean.featurize.Featurizer
import sampleclean.clean.featurize.Tokenizer._

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

private [sampleclean] class CrowdMatcherChooser(scc: SampleCleanContext,
						  eval:Evaluator) extends Serializable {

	def pairToLabeledPoint[K,V](attr:String,
						   row1: Row, 
						   row2:Row, 
						   featurizer:Featurizer, 
						   params: collection.immutable.Map[K,V]=null):LabeledPoint = {
		var label = 0.0
		if (eval.binaryConstraints.contains((row1(0).asInstanceOf[String],
																row2(0).asInstanceOf[String],
																attr)))
			label = 1.0

		return new LabeledPoint(label,
								Vectors.dense(featurizer.featurize(Set(row1, row2),params)._2)
								)
	}

	//FN+FP
	def getCrowdEstimate[K,V](sampleTableName: String, 
						 attr: String, 
						 featurizer:Featurizer,
						 params: collection.immutable.Map[K,V]=null):(Double,Double) = {

		val data = scc.getCleanSample(sampleTableName).rdd.filter( (x:Row) => eval.binaryKeySet.contains(x(0).asInstanceOf[String]))
		val edgeList = data.cartesian(data).map(x => pairToLabeledPoint(attr,x._1,x._2, featurizer, params))
		val model = SVMWithSGD.train(edgeList, 100, 1, 1, 1)
		return trainError(model, edgeList, edgeList.count)
	}


  	def trainError(model: SVMModel, trainingData: RDD[LabeledPoint], trainN: Long): (Double, Double) = {
    val labelAndPreds = trainingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    (labelAndPreds.filter(r => (r._1 != r._2 && r._1 == 1)).count().toDouble / trainN,
     labelAndPreds.filter(r => (r._1 != r._2 && r._1 == 0)).count().toDouble / trainN)
  }

}