package sampleclean.activeml


import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, Server, ServerBuilder}
import com.twitter.finagle.http._
import com.twitter.finagle.http.service.RoutingService
import com.twitter.util.{Future => TFuture, Await}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpResponseStatus}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{write => swrite}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/**
  * An individual result from the crowd.
  * @param identifier the item's unique identifier.
  * @param value the value returned by the crowd
  */
case class CrowdResultItem(identifier:String, value:Double)

/**
  * A batch of results from the crowd.
  * @param group_id the unique identifier for the batch.
  * @param answers a list of individual results in the batch.
  */
case class CrowdResult(group_id: String, answers: List[CrowdResultItem])

/** A webserver that requests data from the crowd and waits for results asynchronously */
object CrowdHTTPServer {
  private val crowdJobURL = "crowds/%s/tasks/"

  // Remember which point ids were in which group.
  private val groupMap = new mutable.HashMap[String, Set[String]]()

  // Store results for each group before they are all finalized.
  private val intermediateResults = new ConcurrentHashMap[String, CrowdResult]()

  // Promises for the final group results.
  private val finalResults = new ConcurrentHashMap[String, Promise[CrowdResult]]()

  private var running = false
  private var server: Server = null

  // Build the finagle service to process incoming data.
  private val crowdResultService = new Service[Request, Response] {
    def apply(req: Request): TFuture[Response] = {
      // parse the request JSON
      implicit val formats = DefaultFormats
      val rawJSON = req.getParam("data")
      println("[SampleClean] Received CrowdLabels")
     // println("GOT DATA FROM THE CROWD! Data: " + rawJSON)
      val result = parse(rawJSON).extract[CrowdResult]
      //println("Parsed: " + result)

      // handle the crowdresult
      handleResult(result)

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
   * @param groupId a unique id for the group of points.
   * @param points the points to label, with enough context to label them.
   * @param groupContext context shared by all points in the group.
   * @param parameters configuration for the crowd service.
   * @return a future that will eventually hold the crowd's response.
   */
  def makeRequest(groupId: String, points:Seq[(String, PointLabelingContext)], groupContext: GroupLabelingContext,
                  parameters: CrowdLabelGetterParameters): Future[CrowdResult] = {

    // Make sure the server is running
    if (!running) {
      start(parameters.responseServerPort)
    }

    // Generate JSON according to the crowd server's API
    implicit val formats = Serialization.formats(NoTypeHints)
    val pointsJSON = (points map {point => point._1 -> parse(swrite(point._2.content))}).toMap
    val groupContextJSON = parse(swrite(groupContext.data))
    val requestData = compact(render(
      ("configuration" ->
        ("task_type" -> groupContext.taskType) ~
          ("task_batch_size" -> parameters.maxPointsPerHIT) ~
          ("num_assignments" -> parameters.maxVotesPerPoint) ~
          ("callback_url" -> ("http://" + parameters.responseServerHost + ":" + parameters.responseServerPort))) ~
        ("group_id" -> groupId) ~
        ("group_context" -> groupContextJSON) ~
        ("content" -> pointsJSON)))
    //println("Request JSON: " + requestData)

    // Send the request to the crowd server.
    //println("Issuing request...")
    val use_ssl = sys.env.getOrElse("SSL", "0") == "1"
    val builder = ClientBuilder()
      .codec(Http())
      .hosts(parameters.crowdServerHost + ":" + parameters.crowdServerPort)
      .hostConnectionLimit(1)

    val client: Service[HttpRequest, HttpResponse] = if (use_ssl) builder.tlsWithoutValidation().build() else builder.build()

    val url_scheme = if (use_ssl) "https" else "http"
    val request = RequestBuilder()
      .url(url_scheme + "://" + parameters.crowdServerHost + ":" + parameters.crowdServerPort + "/" + crowdJobURL format parameters.crowdName)
      .addHeader("Charset", "UTF-8")
      .addFormElement(("data", requestData))
      .buildFormPost()
    val responseFuture = client(request)

    // Check that our crowd request was successful. The response data will be handled by handleResponse()
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

    Await.result(responseFuture)

    // Register the point ids in the group map.
    groupMap.put(groupId, points map {p => p._1} toSet)

    // Create a promise for the final result
    val crowdResponse = Promise[CrowdResult]()
    finalResults.put(groupId, crowdResponse)

    // Return the promise
    crowdResponse.future
  }

  /**
   * Stores new crowd responses in the cache and resolves futures once the whole group has been labeled.
   * @param result new crowd response.
   */
  def handleResult(result: CrowdResult) {
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
      val labelPromise = finalResults.get(groupId)
      if (labelPromise != null) {
        labelPromise success newResults
      } else {
        println("Invalid group id: " + groupId)
      }
      // Clean up groupMap and intermediateResults.
      // TODO(dhaas): can we also clean up the finalResults map?
      groupMap.remove(groupId)
      intermediateResults.remove(groupId)
    } else {
      //println("Not done with group yet! Awaiting " + (groupMap.getOrElse(groupId, null).size - newResults.answers.size) + " labels.")
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
