package sampleclean.crowd

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import sampleclean.activeml.{LabelGetter, utils}

import scala.concurrent._
import scala.concurrent.duration.Duration

/**
 * The abstract class used to represent the different contexts for different crowd tasks
 * There is only one field data of type T, which stores the necessary information for a context
 */
abstract class PointLabelingContext {
  def content : Object
}


/** An abstract class used to represent the group labeling contexts */
abstract class GroupLabelingContext {
  def taskType : String
  def data : Object
}

/**
  * Parameters for the crowd label getter.
  * @param responseServerPort port on which to run the response webserver.
  * @param responseServerHost host on which to run the response webserver.
  * @param crowdServerPort port on which crowd server runs.
  * @param crowdServerHost host on which crowd server runs.
  * @param maxPointsPerHIT maximum number of points to batch in a single Amazon Mechanical Turk HIT.
  * @param maxVotesPerPoint maximum number of crowd workers to vote on a single point's label.
  */
case class CrowdLabelGetterParameters
(
  responseServerPort: Int=8082,
  responseServerHost: String="127.0.0.1",
  crowdName: String="internal",
  crowdServerPort: Int=8000,
  crowdServerHost: String="127.0.0.1",
  maxPointsPerHIT: Int=5,
  maxVotesPerPoint: Int=1,
  crowdConfig: Object=Map()
  )


/**
  * Gets labels from the crowd service by posting them as HITs to Amazon Mechanical Turk.
  * @param parameters parameters for this class.
  * @constructor create a new label getter with the passed parameters.
  */
class CrowdLabelGetter(parameters: CrowdLabelGetterParameters) extends LabelGetter[PointLabelingContext, GroupLabelingContext, CrowdLabelGetterParameters](parameters){

  // Make sure the web server has started
  CrowdHTTPServer.start(parameters.responseServerPort)

  /**
    * Asynchronously get labels for a group of points from the crowd.
    * @param points an RDD of unlabeled points (id, feature vector, point context).
    * @param groupContext group labeling context shared by all points.
    * @return an RDD of labeled points (id, feature vector)
    */
  def addLabels(points: RDD[(String, Vector, PointLabelingContext)], groupContext: GroupLabelingContext): RDD[(String, LabeledPoint)] = {
    // TODO: smarter caching--take advantage of equivalence in point + context

    // generate an id for this group of points
    val groupId = utils.randomUUID()
    //println("NEW ACTIVE LEARNING BATCH: id=" + groupId)

    // gather the points and store their vectors, since the number should be small
    val pointGroup = points.collect()
    val pointMap = (pointGroup map {p => p._1 -> p._2}).toMap
    val crowdFuture = CrowdHTTPServer.makeRequest(groupId, pointGroup map {p => p._1 -> p._3}, groupContext, parameters)

    // Block until the group is ready.
    val crowdResult = Await.result(crowdFuture, Duration.Inf)
    //println("REQUEST COMPLETE: id=" + groupId)

    // Extract and return the labeled points as an RDD
    val labeledPoints = crowdResult.answers map { answer =>
      val vector = pointMap.getOrElse(answer.identifier, null)
      if (vector == null) {
        throw new RuntimeException("Crowd Result received for invalid point id: " + answer.identifier)
      }
      answer.identifier -> new LabeledPoint(answer.value, vector)
    }
    points.sparkContext.parallelize(labeledPoints)
  }

  /** Stop the web server once labeling is complete. */
  def cleanUp() {
    CrowdHTTPServer.stop()
  }
}
