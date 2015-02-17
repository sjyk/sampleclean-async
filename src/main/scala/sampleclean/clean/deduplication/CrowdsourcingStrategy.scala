package sampleclean.clean.deduplication

import org.apache.spark.rdd.RDD
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationGroupLabelingContext, DeduplicationPointLabelingContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * This class is used to request crowd participation
 */
case class CrowdsourcingStrategy() {
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

  def run(points:RDD[(String, DeduplicationPointLabelingContext)],
          groupContext: DeduplicationGroupLabelingContext): RDD[(String, Double)] = {

    crowdTask.processBlocking(points, groupContext, getTaskParameters)
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

