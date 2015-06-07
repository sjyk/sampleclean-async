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
import edu.stanford.math.plex._


private [sampleclean] class PersistentHomologyThresholdTuner(scc: SampleCleanContext,
										simfeature: AnnotatedSimilarityFeaturizer) extends Serializable {

	def rowsToSimilarity[K,V](rows:Set[Any], params: collection.immutable.Map[K,V]=null):Double = {
		return simfeature.getSimilarityDouble(rows.asInstanceOf[Set[Row]],params)._2
	}

	def tuneThreshold(sampleTableName: String):Double = {
		val data = scc.getCleanSample(sampleTableName)
		
		//todo add error handling clean up
		var tokenWeights = collection.immutable.Map[String, Double]()
      	var tokenCounts = collection.immutable.Map[String, Int]()
      	tokenCounts = computeTokenCount(data.map(simfeature.tokenizer.tokenize(_, simfeature.getCols())))
      	tokenWeights = tokenCounts.map(x => (x._1, math.log10(data.count.toDouble / x._2)))

      	//map reduce tasks to get the data into the matrix
		val dataMatrix = data.rdd
							.cartesian(data.rdd)
							.map(x => (x._1(0).toString(), 
									      (x._2(0).toString(),
									       rowsToSimilarity(x.productIterator.toSet, tokenWeights))))
							.groupByKey()
							.sortByKey()
							.map(x => x._2.toArray
										  .sortBy(_._1)
										  .map(x => (1.0 - x._2)))
							.collect()
							.toArray


		var pdatarelative = Plex.DistanceData(dataMatrix);
		var ripsrelative = Plex.RipsStream(0.01,3,1.0,pdatarelative);
		var intervals = Plex.Persistence().computeIntervals(ripsrelative);
		
		for(i <- intervals)
			{
				println(i.dimension+" " + i.toDouble().mkString(" "))
			}

		return 0.0
	}

	def getCandidatePairsCount(sampleTableName: String, thresh:Double):Long = {
		val data = scc.getCleanSample(sampleTableName)
		return data.rdd.cartesian(data.rdd).map(x => rowsToSimilarity(x.productIterator.toSet)).filter(x => x > thresh).count()
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