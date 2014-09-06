package sampleclean.activeml


import java.net.InetSocketAddress

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http._
import com.twitter.finagle.http.service.RoutingService
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json4s._
import org.json4s.jackson.JsonMethods._

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

/** A simple webserver that waits for data from the crowd and sends it to a callback function. */
object CrowdHTTPServer {
  private var running = false
  private var server: Server = null
  private var callback: CrowdResult => Unit = null

  // build the finagle service to process incoming data
  val crowdResultService = new Service[Request, Response] {
    def apply(req: Request): Future[Response] = {
      // parse the request JSON
      implicit val formats = DefaultFormats
      val rawJSON = req.getParam("data")
      println("GOT DATA FROM THE CROWD! Data: " + rawJSON)
      val result = parse(rawJSON).extract[CrowdResult]
      println("Parsed: " + result)

      // send it to the callback function
      if (callback != null) callback(result)

      // acknowledge the response
      val res = Response(req)
      res.setStatus(HttpResponseStatus.OK)
      Future.value(res)
    }
  }

  // Accept data at the top-level path.
  val service = RoutingService.byPath {
    case "/" => crowdResultService
  }

  // create the server.
  val builder = ServerBuilder()
    .codec(new RichHttp[Request](Http.get()))
    .name("CrowdHttpServer")
    //.daemon(true)

  /**
    * Sets the callback invoked on crowd responses.
    * @param newCallback callback function that takes a crowd result processes it, and returns nothing. Callback will be
    *                    run from the web-server thread, not the thread that calls this method.
    */
  def setCallback(newCallback: CrowdResult => Unit) {
    callback = newCallback
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
