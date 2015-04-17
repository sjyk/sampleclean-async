package sampleclean.eval
import sampleclean.clean.deduplication.join._
import sampleclean.clean.deduplication.blocker._
import sampleclean.clean.deduplication.matcher._
import sampleclean.clean.deduplication._
import sampleclean.api.SampleCleanContext
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import org.apache.spark.rdd.RDD


private [sampleclean] class MonotonicSimilarityThresholdTuner(scc: SampleCleanContext,
										eval:Evaluator,
										simfeature: AnnotatedSimilarityFeaturizer) extends Serializable {

	var tree : scala.collection.mutable.Map[String, Set[(String, Double)]]  = scala.collection.mutable.Map()

	def rowsToSimilarity[K,V](rows:Set[Any], params: collection.immutable.Map[K,V]=null):Double = {
		return simfeature.getSimilarityDouble(rows.asInstanceOf[Set[Row]],params)._2
	}

	def addEdge(edge:(Double,(Row,Row))) = {

		val h1 = edge._2._1(0).toString()
		val h2 = edge._2._2(0).toString()

		if(! tree.contains(h1))
			tree(h1) = Set()

		if(! tree.contains(h2))
			tree(h2) = Set()

		if(! dfs(h1,h2,Set())){
			tree(h1) += ((h2,edge._1))
			tree(h2) += ((h1,edge._1))
		}
	}

	def dfs(start:String, end:String, traverseSet:Set[String]):Boolean ={
		if(start == end)
			return true
		else
		{
			var result = false
			for(t <- tree(start))
			{
				if(!traverseSet.contains(t._1))
					result = result || dfs(t._1,end,traverseSet + t._1)
			}
			return result
		}
	}

	def tuneThreshold(sampleTableName: String):Double = {
		val data = scc.getCleanSample(sampleTableName).filter(x => eval.binaryKeySet.contains(x(0).asInstanceOf[String]))
		//todo add error handling clean up
		var tokenWeights = collection.immutable.Map[String, Double]()
      	var tokenCounts = collection.immutable.Map[String, Int]()
      	tokenCounts = computeTokenCount(data.map(simfeature.tokenizer.tokenize(_, simfeature.getCols())))
      	tokenWeights = tokenCounts.map(x => (x._1, math.log10(data.count.toDouble / x._2)))

		val edgeList = data.cartesian(data).map(x => 
												(rowsToSimilarity(x.productIterator.toSet, tokenWeights), (x._1, x._2)))
							.filter(x => x._1 > 1e-6)
							.filter(x => eval.binaryConstraints.contains( (x._2._1(0).asInstanceOf[String],
																		   x._2._2(0).asInstanceOf[String], 
							simfeature.colNames(0))))
							.sortByKey(false).collect()

		for(edge <- edgeList)
		{
			addEdge(edge)
		}

		var min = 1.0
		for(t <- tree) {
			for (j <- tree(t._1))
				if(j._2 < min)
					min = j._2
		}

		return min
		//println(reachableSet(tree.keySet.last, Set()))
	}

	def getCandidatePairsCount(sampleTableName: String, thresh:Double):Long = {
		val data = scc.getCleanSample(sampleTableName)
		return data.cartesian(data).map(x => rowsToSimilarity(x.productIterator.toSet)).filter(x => x > thresh).count()
	}

	def computeTokenCount(data: RDD[(Seq[String])]): collection.immutable.Map[String, Int] = {
    val m = data.flatMap{
      case tokens =>
        for (x <- tokens.distinct)
        yield (x, 1)
    }.reduceByKeyLocally(_ + _)
    collection.immutable.Map(m.toList: _*)
  	}
}