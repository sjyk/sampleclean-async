package sampleclean.clean.deduplication.matcher

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.deduplication.CrowdsourcingStrategy

import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}


class CrowdMatcher(scc: SampleCleanContext, 
                            sampleTableName: String,
                            crowdsourcingStrategy:CrowdsourcingStrategy,
                            onReceiveNewMatches: RDD[(Row,Row)] => Unit) extends
							    Matcher(scc, sampleTableName) {

  def matchPairs(candidatePairs:RDD[(Row,Row)]): RDD[(Row,Row)] = {

      val candidatesWithSortKeys = candidatePairs.map { pair => -math.min(pair._1.getLong(1), pair._2.getLong(1)) -> pair }
      val sortedCandidates = candidatesWithSortKeys.sortByKey().map{kv => kv._2}
      //var candidatePairsArray = candidatePairs.collect().sortBy(pair => -math.min(pair._1.getLong(1), pair._2.getLong(1)))

      println("[SampleClean] Publish %d pairs to AMT".format(sortedCandidates.count()))
      //val crowdsourcingStrategy = params.get("crowdsourcingStrategy").asInstanceOf[CrowdsourcingStrategy]

      //todo fix
      val groupContext = DeduplicationGroupLabelingContext(
        taskType="er", data=Map("fields" ->List("attr", "count")))

      // Assign a unique id for each candidate pair
      val candidatePairsWithId = sortedCandidates.map{ pair =>
        val random_id = utils.randomUUID()
        (random_id, pair)
      }.cache()
      val contextMap = candidatePairsWithId.collect().toMap

      def onNewCrowdResult(results: Seq[(String, Double)]) {
        val candidatePairsArray = scc.getSparkContext().parallelize(
          results.withFilter(_._2 > 0.5).map{ answer =>
          assert(contextMap.contains(answer._1))
          contextMap.apply(answer._1)
        })

        onReceiveNewMatches(candidatePairsArray)
      }

      // Construct the point labeling context with a unique id for each point
      val crowdData = candidatePairsWithId.map { case (id, (row1, row2)) =>
        val entity1Data = List(row1.getString(0), row1.getLong(1))
        val entity2Data = List(row2.getString(0), row2.getLong(1))
        val context = DeduplicationPointLabelingContext(content=List(entity1Data, entity2Data))
        (id, context)
      }
      
      //it is async by default
      crowdsourcingStrategy.asyncRun(crowdData, groupContext, onNewCrowdResult)

      return scc.getSparkContext().parallelize(new Array[(Row, Row)](0))
  }	

  def matchPairs(candidatePairs: => RDD[Set[Row]]): RDD[(Row,Row)] = {
      return matchPairs(candidatePairs.flatMap(selfCartesianProduct))
  }

}
