package sampleclean.activeml

import java.util.concurrent.ConcurrentHashMap

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, RequestBuilder}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpResponseStatus}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write => swrite}

import scala.collection.mutable
import scala.concurrent.{Promise, _}
import scala.concurrent.duration.Duration

/**
 * The abstract class used to represent the different contexts for different crowd tasks
 * There is only one field data of type T, which stores the necessary information for a context
 */
abstract class PointLabelingContext {
  def content : Object
}

/**
  * Parameters for labeling a single tweet
  * @param content the text of the tweet.
  */
case class SentimentPointLabelingContext(content : String) extends PointLabelingContext

/**
 * Parameters for labeling a entity resolution task
 * @param content a list of two lists, which contains the corresponding values of each record
 */
case class DeduplicationPointLabelingContext(content : List[List[Any]]) extends PointLabelingContext


/** An abstract class used to represent the group labeling contexts */
abstract class GroupLabelingContext {
  def taskType : String
  def data : Object
}

/**  Group Context subclass for sentiment analysis, the data should be an empty map
* @param taskType the type of all the points in the group
* @param data the group context that is shared among all the points
*/
case class SentimentGroupLabelingContext(taskType : String, data : Map[Any, Any]) extends GroupLabelingContext


/** Group Context subclass for deduplication
  * @param taskType the type of all the points in the group
  * @param data the group context that is shared among all the points
  * */
case class DeduplicationGroupLabelingContext(taskType : String, data : Map[String, List[String]])
  extends GroupLabelingContext


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
  responseServerPort: Int=8080,
  responseServerHost: String="127.0.0.1",
  crowdServerPort: Int=8000,
  crowdServerHost: String="127.0.0.1",
  maxPointsPerHIT: Int=5,
  maxVotesPerPoint: Int=1
  )

/** Companion object for the crowd label getter. Ensures that the web server is running and the callback is set up. */
object CrowdLabelGetter {
  val crowdJobURL = "amt/hitsgen/"

  // Set up a cache for returned crowd labels
  val groupMap = new mutable.HashMap[String, Set[String]]()
  val intermediateResults = new ConcurrentHashMap[String, CrowdResult]()
  val labelCache = new ConcurrentHashMap[String, Promise[CrowdResult]]()

  /**
    * Stores new crowd labels in the cache and resolves futures once the whole group has been labeled.
    * @param result new crowd label.
    */
  def storeCrowdResult(result: CrowdResult) {
    // update the intermediate results for this group
    val groupId = result.group_id
    var newResults:CrowdResult = null
    if (!intermediateResults.containsKey(groupId)) {
      newResults = result
    } else {
      val currentResults = intermediateResults.get(groupId)
      newResults = new CrowdResult(groupId, currentResults.answers ++ result.answers)
    }
    intermediateResults.put(groupId, newResults)

    // if we have results for every point in the group, return.
    // Note: could implement other strategies here (e.g., early return).
    if (newResults.answers.map(answer => answer.identifier).toSet equals groupMap.getOrElse(groupId, null)) {
      val labelPromise = labelCache.get(groupId)
      if (labelPromise != null) {
        labelPromise success newResults
      } else {
        //println("Invalid group id: " + groupId)
      }
      // Clean up groupMap and intermediateResults.
      groupMap.remove(groupId)
      intermediateResults.remove(groupId)
    } else {
      //println("Not done with group yet! Awaiting " + (groupMap.getOrElse(groupId, null).size - newResults.answers.size) + " labels.")
    }
  }
  CrowdHTTPServer.setCallback(storeCrowdResult)
}

/**
  * Gets labels from the crowd service by posting them as HITs to Amazon Mechanical Turk.
  * @param parameters parameters for this class.
  * @constructor create a new label getter with the passed parameters.
  */
class CrowdLabelGetter(parameters: CrowdLabelGetterParameters) extends LabelGetter[PointLabelingContext, GroupLabelingContext, CrowdLabelGetterParameters](parameters){

  // Make sure the web server has started
  CrowdHTTPServer.start(parameters.responseServerPort)

  /**
    * Send a group of points to the crowd service for asynchronous labeling.
    * @param groupId a unique id for the group of points.
    * @param points the points to label, with enough context to label them.
    * @param groupContext context shared by all points in the group.
    */
  def startLabelRequest(groupId:String, points:Seq[(String, Vector, PointLabelingContext)], groupContext: GroupLabelingContext) {

    // Generate JSON according to the crowd server's API
    implicit val formats = Serialization.formats(NoTypeHints)
    val pointsJSON = (points map {point => point._1 -> parse(swrite(point._3.content))}).toMap
    val groupContextJSON = parse(swrite(groupContext.data))
    val requestData = compact(render(
      ("configuration" ->
        ("type" -> groupContext.taskType) ~
          ("hit_batch_size" -> parameters.maxPointsPerHIT) ~
          ("num_assignments" -> parameters.maxVotesPerPoint) ~
          ("callback_url" -> ("http://" + parameters.responseServerHost + ":" + parameters.responseServerPort))) ~
        ("group_id" -> groupId) ~
        ("group_context" -> groupContextJSON) ~
        ("content" -> pointsJSON)))
    //println("Request JSON: " + requestData)

    // Send the request to the crowd server.
    //println("Issuing request...")
    val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
      .codec(Http())
      .hosts(parameters.crowdServerHost + ":" + parameters.crowdServerPort)
      .hostConnectionLimit(1)
      .tlsWithoutValidation() // TODO: ONLY IN DEVELOPMENT
      .build()
    val request = RequestBuilder()
      .url("https://" + parameters.crowdServerHost + ":" + parameters.crowdServerPort + "/" + CrowdLabelGetter.crowdJobURL)
      .addHeader("Charset", "UTF-8")
      .addFormElement(("data", requestData))
      .buildFormPost()
    val responseFuture = client(request)

    // Check that our crowd request was successful. The response data will be handled by CrowdHTTPServer.callback
    responseFuture onSuccess { resp: HttpResponse =>
      val responseData = resp.getContent.toString("UTF-8")
      resp.getStatus  match {
        case HttpResponseStatus.OK =>
          implicit val formats = DefaultFormats
          (parse(responseData) \ "status").extract[String] match {
            case "ok" =>  println("[SampleClean] Created AMT HIT")
            case other: String => println("Error! Bad request: " + other)
          }
        case other: HttpResponseStatus =>
          println("Error! Got unexpected response status " + other.getCode + ". Data: " + responseData)
      }

    } onFailure { exc: Throwable =>
      println("Failure!")
      throw exc
    }
  }

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
    System.out.flush()

    // gather the points and store their vectors, since the number should be small
    val pointGroup = points.collect()
    val pointMap = (pointGroup map {p => p._1 -> p._2}).toMap

    // Register the point ids in the group map.
    CrowdLabelGetter.groupMap.put(groupId, pointGroup map {p => p._1} toSet)

    // Register a promise for the group.
    val crowdResponse = Promise[CrowdResult]()
    val crowdFuture = crowdResponse.future
    CrowdLabelGetter.labelCache.put(groupId, crowdResponse)

    // Send the group to the crowd for asynchronous processing.
    startLabelRequest(groupId, pointGroup, groupContext)

    // Block until the group is ready.
    val crowdResult = Await.result(crowdFuture, Duration.Inf)
    //println("REQUEST COMPLETE: id=" + groupId)
    System.out.flush()

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
