package sampleclean.clean.deduplication

import sampleclean.activeml._
import sampleclean.crowd._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
 * This class is used to request crowd participation
 */
case class CrowdsourcingStrategy() {
  var labelGetterParameters = CrowdLabelGetterParameters() // Use defaults

  def setCrowdLabelGetterParameters(labelGetterParameters: CrowdLabelGetterParameters): CrowdsourcingStrategy = {
    this.labelGetterParameters = labelGetterParameters
    return this
  }
  def getCrowdLabelGetterParameters(): CrowdLabelGetterParameters = {
    return this.labelGetterParameters
  }

  def run(points:Seq[(String, PointLabelingContext)],
          groupContext: GroupLabelingContext): CrowdResult = {

    val groupId = utils.randomUUID() // random group id for the request.
    val resultFuture = CrowdHTTPServer.makeRequest(groupId, points, groupContext, labelGetterParameters)

    // wait for the crowd to finish
    return Await.result(resultFuture, Duration.Inf)
  }

  def asyncRun(points:Seq[(String, PointLabelingContext)],
               groupContext: GroupLabelingContext,
               onNewCrowdResult: CrowdResult => Unit) = {

    println(points.size)

    // Split points into small groups of points; Collect crowd results asynchronously
    val resultFutures = points.grouped(labelGetterParameters.maxPointsPerHIT).toSeq.map{ smallPoints =>
      println(smallPoints.size)
      val groupId = utils.randomUUID() // random group id for the request.
      val resultFuture = CrowdHTTPServer.makeRequest(groupId, smallPoints, groupContext, labelGetterParameters)
      resultFuture.onComplete {
        case Success(result) => onNewCrowdResult(result)
        case Failure(e) => e.printStackTrace
      }
      resultFuture
    }
    val futureResults = Future.sequence(resultFutures)

    // wait until all future results are completed
    Await.ready(futureResults, Duration.Inf)
  }
}

