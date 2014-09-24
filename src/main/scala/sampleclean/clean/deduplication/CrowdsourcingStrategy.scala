package sampleclean.clean.deduplication

import sampleclean.activeml._
import scala.concurrent.{Await}
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
}

