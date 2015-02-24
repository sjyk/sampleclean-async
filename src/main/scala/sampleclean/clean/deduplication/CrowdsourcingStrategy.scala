package sampleclean.clean.deduplication

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd._
import org.apache.spark.sql._
import sampleclean.activeml.{ActiveLearningParameters, SVMParameters, _}
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationGroupLabelingContext, DeduplicationPointLabelingContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import sampleclean.activeml.SVMParameters
import scala.Some
import sampleclean.activeml.ActiveLearningParameters
import org.apache.spark.mllib.regression.LabeledPoint
import sampleclean.clean.featurize.Featurizer


/**
 * This class is used to request crowd participation
 */
case class CrowdsourcingStrategy(displayedColNames: List[String], featurizer:Featurizer) {
  var crowdParameters = CrowdConfiguration() // Use defaults
  var taskParameters = CrowdTaskConfiguration() // Use defaults
  private val crowdTask = new DeduplicationTask()

  def setCrowdParameters(crowdParams: CrowdConfiguration): CrowdsourcingStrategy = {
    this.crowdParameters = crowdParams
    this.crowdTask.configureCrowd(crowdParams)
    this
  }
  
  def getCrowdParameters: CrowdConfiguration = {
    this.crowdParameters
  }

  def setTaskParameters(taskParams: CrowdTaskConfiguration): CrowdsourcingStrategy = {
    this.taskParameters = taskParams
    this
  }

  def getTaskParameters: CrowdTaskConfiguration = {
    this.taskParameters
  }

  def asyncRun(points:RDD[(String, DeduplicationPointLabelingContext)],
               groupContext: DeduplicationGroupLabelingContext,
               onNewCrowdResult: Seq[(String, Double)] => Unit) = {

    println(points.count())
    val resultFuture = crowdTask.processStreaming(points, groupContext, getTaskParameters)
    resultFuture.onBatchProcessed(onNewCrowdResult, getTaskParameters.maxPointsPerTask)

    // wait until all future results are completed
    Await.ready(resultFuture, Duration.Inf)
  }

}

