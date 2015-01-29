package sampleclean.simjoin
import sampleclean.clean.featurize.BlockingFeaturizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

class SimilarityJoin(blocker: BlockingFeaturizer, 
					 projection:List[Int], 
					 weighted:Boolean = false) extends Serializable{


	/* The default implementation is naive but subclasses should override 
	 * optimize
	 */
	def join(rddA: RDD[Row], 
			 rddB:RDD[Row], 
			 smallerA:Boolean = true, 
			 containment:Boolean = true): RDD[(Row,Row)] = {

		var tokenWeights = collection.immutable.Map[String,Double]()
		var tokenCounts = collection.immutable.Map[String,Int]()
		var tableSize = 1L

		if(weighted){
			if (smallerA && containment){
				tokenCounts = computeTokenCount(rddB.map(blocker.tokenizer.tokenize(_,projection)))
				tableSize = rddB.count()
			}
			else if (containment){
				tokenCounts = computeTokenCount(rddA.map(blocker.tokenizer.tokenize(_,projection)))
				tableSize = rddA.count()
			}
			else{
				tokenCounts = computeTokenCount(rddA.union(rddB).map(blocker.tokenizer.tokenize(_,projection)))
				tableSize = rddA.union(rddB).count()
			}

			tokenWeights = tokenCounts.map(x => (x._1, math.log10(tableSize.toDouble / x._2)))
		}

		val featurized = rddA.cartesian(rddB).map(x => blocker.featurize(Set(x._1,x._2),tokenWeights))
		return featurized.filter(x => (x._2(0) == 1.0)).map(x => (x._1.head, x._1.last))

	}

	/**
   	* Counts the number of times that each token shows up in the data
   	* @param data  RDD with tokenized records.
   	*/
  	private def computeTokenCount(data: RDD[(Seq[String])]): collection.immutable.Map[String, Int] = {
    	val m = data.flatMap{
      	case tokens =>
        	for (x <- tokens.distinct)
        	yield (x, 1)
    	}.reduceByKeyLocally(_ + _)

    	collection.immutable.Map(m.toList: _*)
  	}

}