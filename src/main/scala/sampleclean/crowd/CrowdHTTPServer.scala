package sampleclean.crowd

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.finagle.http._
import com.twitter.finagle.http.service.RoutingService
import com.twitter.util.{Await, Future => TFuture}
import org.apache.spark.rdd.RDD
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpResponseStatus}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{write => swrite}
import sampleclean.activeml.utils
import sampleclean.crowd.context.{PointLabelingContext, GroupLabelingContext}

import scala.collection.mutable


/**
 * An abstract individual result from the crowd.
 */
abstract class CrowdResultItem {
  val identifier:String
  val value:Any
}

/**
 * A crowd result containing a double
 * @param identifier the id of the result point
 * @param value the value of the result point.
 */
case class CrowdResultDouble(identifier:String, value:Double) extends CrowdResultItem

/**
 * A batch of results from the crowd.
 * @param group_id the unique identifier for the batch.
 * @param answers a list of individual results in the batch.
 * @tparam T the type of individual results.
 */
case class CrowdResult[T <: CrowdResultItem](group_id: String, answers: List[T])


/** A webserver that requests data from the crowd and waits for results asynchronously */
object CrowdHTTPServer {
  private final val crowdJobURL = "crowds/%s/tasks/"
  private final val taskTypeMap = Map[String, Class[_]](
    "er" -> classOf[Double],
    "sa" -> classOf[Double]
  )

  // Store result objects for in-process groups
  private val results = new ConcurrentHashMap[String, AsyncCrowdResult[_]]()

  // Remember which point ids were in which group.
  private val groupMap = new ConcurrentHashMap[String, Set[String]]()

  // Remember what type of task each group is.
  private val groupTypes = new mutable.HashMap[String, String]

  private var running = false
  private var server: Server = null

  // Build the finagle service to process incoming data.
  private val crowdResultService = new Service[Request, Response] {
    def apply(req: Request): TFuture[Response] = {
      // parse the request JSON
      //implicit val formats = DefaultFormats
      implicit val format = Serialization.formats(NoTypeHints)
      val rawJSON = req.getParam("data")
      println("[SampleClean] Received CrowdLabels")
     // println("GOT DATA FROM THE CROWD! Data: " + rawJSON)

      // get the group id out to figure out the data type
      val parsedJSON = parse(rawJSON)
      val groupId = (parsedJSON \ "group_id").extract[String]
      val taskType = groupTypes.getOrElse(groupId, "")
      val resultType = taskTypeMap.get(taskType)

      // handle the result
      resultType match {
        case Some(d) if d == classOf[Double] => handleResult(parsedJSON.extract[CrowdResult[CrowdResultDouble]])
        case None => throw new RuntimeException("Unkonwn groupID: " + groupId + " or task type: " + taskType)
        case _ => throw new RuntimeException("Invalid crowd datatype: " + resultType)
      }

      // acknowledge the response
      val res = Response(req)
      res.setStatus(HttpResponseStatus.OK)
      TFuture.value(res)
    }
  }

  // Accept data at the top-level path.
  private val service = RoutingService.byPath {
    case "/" => crowdResultService
  }

  // create the server.
  private val builder = ServerBuilder()
    .codec(new RichHttp[Request](Http.get()))
    .name("CrowdHttpServer")
    //.daemon(true)

  /**
   * Send a group of points to the crowd service for asynchronous processing.
   * @param inputData the points to label, with enough context to label them.
   * @param groupContext context shared by all points in the group.
   * @param crowdConfiguration configuration for the crowd service.
   * @param taskConfiguration configuration options for this group of tasks.
   * @tparam C class for individual point context.
   * @tparam G class for group context.
   * @tparam O type of data returned by the crowd.
   * @return a future that will eventually hold the crowd's response.
   */
  def makeRequest[C <: PointLabelingContext, G <: GroupLabelingContext, O]
  (inputData:RDD[(String, C)], groupContext: G,
   crowdConfiguration: CrowdConfiguration,
   taskConfiguration: CrowdTaskConfiguration): AsyncCrowdResult[O] = {

    // Make sure the server is running
    if (!running) {
      start(crowdConfiguration.responseServerPort)
    }

    // collect the points but save their SparkContext
    implicit val sc = inputData.sparkContext
    val points = inputData.collect()

    // Generate a random id for the group and register it with a result object
    val groupId = utils.randomUUID()
    //val resultObjectClass = classOf[AsyncCrowdResult[O]]
    //val resultObject = resultObjectClass.newInstance()
    val resultObject = new AsyncCrowdResult[O]()
    results.put(groupId, resultObject)
    groupTypes.put(groupId, groupContext.taskType)

    // Register the point ids in the group map.
    groupMap.put(groupId, points map {p => p._1} toSet)

    // Generate JSON according to the crowd server's API
    implicit val formats = Serialization.formats(NoTypeHints)
    val pointsJSON = (points map {point => point._1 -> parse(swrite(point._2.content))}).toMap
    val groupContextJSON = parse(swrite(groupContext.data))
    val crowdConfigJSON = parse(swrite(taskConfiguration.crowdTaskOptions))
    val requestData = compact(render(
      ("configuration" ->
        ("task_type" -> groupContext.taskType) ~
          ("task_batch_size" -> taskConfiguration.maxPointsPerTask) ~
          ("num_assignments" -> taskConfiguration.votesPerPoint) ~
          (crowdConfiguration.crowdName -> crowdConfigJSON) ~
          ("callback_url" -> ("http://" + crowdConfiguration.responseServerHost + ":" + crowdConfiguration.responseServerPort))) ~
        ("group_id" -> groupId) ~
        ("group_context" -> groupContextJSON) ~
        ("content" -> pointsJSON)))
    //println("Request JSON: " + requestData)

    // Send the request to the crowd server.
    //println("Issuing request...")
    val use_ssl = sys.env.getOrElse("SSL", "0") == "1"
    val builder = ClientBuilder()
      .codec(Http())
      .hosts(crowdConfiguration.crowdServerHost + ":" + crowdConfiguration.crowdServerPort)
      .hostConnectionLimit(1)

    val client: Service[HttpRequest, HttpResponse] = if (use_ssl) builder.tlsWithoutValidation().build() else builder.build()

    val url_scheme = if (use_ssl) "https" else "http"
    val request = RequestBuilder()
      .url(url_scheme + "://" + crowdConfiguration.crowdServerHost + ":"
           + crowdConfiguration.crowdServerPort + "/"
           + crowdJobURL format crowdConfiguration.crowdName)
      .addHeader("Charset", "UTF-8")
      .addFormElement(("data", requestData))
      .buildFormPost()
    val responseFuture = client(request)

    // Check that our crowd request was successful. The response data will be handled by handleResponse()
    responseFuture onSuccess { resp: HttpResponse =>
      val responseData = resp.getContent.toString("UTF-8")
      println(responseData)
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

    Await.result(responseFuture)

    resultObject
  }


  /**
   * Stores new crowd responses in the result object and cleans up once the whole group has been labeled.
   * @param newResult new crowd response.
   */
  def handleResult[T <: CrowdResultItem](newResult: CrowdResult[T]) {
    // Look up the result object for the new result's group
    val groupId = newResult.group_id
    if (!(groupMap containsKey groupId)) throw new RuntimeException("Invalid groupID from crowd: " + groupId)

    // Scary scala reflection to add the new result to the group object
    val resultObject = results.get(groupId)
    val updateFunc = resultObject.getClass.getMethods.filter(m => m.getName == "tupleProcessed").head
    newResult.answers map {
      answer => updateFunc.invoke(resultObject, answer.identifier -> answer.value)
    }

    // Delete ids with results from the groupMap to track progress
    val newIds = (newResult.answers map { r => r.identifier}).toSet
    groupMap.replace(groupId, groupMap.get(groupId) &~ newIds)

    // if we have results for every point in the group, clean up.
    if (groupMap.get(groupId) isEmpty) {
      resultObject.complete()
      groupMap.remove(groupId)
      results.remove(groupId)
    }
  }

  /**
    * Starts the server if it isn't already running.
   * @param port the port on which to listen.
   */
  def start(port: Int) {
    synchronized {
      if (!running) {
        running = true
        server = builder
          .bindTo(new InetSocketAddress(port))
          .build(CrowdHTTPServer.service)
      }
    }
  }

  /** Stops the server if it is running. */
  def stop() {
    synchronized {
      if (server != null) {
        server.close()
        running = false
      }
    }
  }

}
