package sampleclean.simjoin
import sampleclean.clean.featurize.BlockingFeaturizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

class BroadcastJoin( @transient sc: SparkContext,
					 blocker: BlockingFeaturizer, 
					 projection:List[Int], 
					 weighted:Boolean = false) extends
					 SimilarityJoin(blocker,projection,weighted) {


	/* The default implementation is naive but subclasses should override 
	 * optimize
	 */
	def join(rddA: RDD[Row], 
			 rddB:RDD[Row], 
			 smallerA:Boolean = true, 
			 containment:Boolean = true): RDD[(Row,Row)] = {

		//TODO

	}

}