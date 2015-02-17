package sampleclean.crowd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import sampleclean.crowd.context._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

// Available Crowd Tasks
class DeduplicationTask extends CrowdTask[DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext, Double]
class SentimentTask extends CrowdTask[SentimentPointLabelingContext, SentimentGroupLabelingContext, Double]

/**
 * A task that uses the crowd to process data.
 * @tparam C type of crowd context for data tuples
 * @tparam G type of crowd context for data groups
 * @tparam O type of output data tuples
 */
class CrowdTask[C <: PointLabelingContext, G <: GroupLabelingContext, O]() extends Serializable {
  var crowdConfiguration: CrowdConfiguration = CrowdConfiguration()

  /**
   * Sets config options for the desired crowd.
   * @param configuration configuration settings
   */
  def configureCrowd(configuration: CrowdConfiguration) = { crowdConfiguration = configuration }

  /**
   * Processes a set of input data points using the crowd and returns the output asynchronously.
   * @param inputData an RDD containing a unique ID, input data, and crowd context for each data point.
   * @param groupContext crowd context relevant to every point in inputData
   * @return a [[sampleclean.crowd.AsyncCrowdResult[O] ]] object for generating asynchronous output.
   */
  def processStreaming(inputData: RDD[(String, C)], groupContext: G, options:CrowdTaskConfiguration=CrowdTaskConfiguration()): AsyncCrowdResult[O] = {
    CrowdHTTPServer.makeRequest[C, G, O](inputData, groupContext, crowdConfiguration, options)
  }

  /**
   * Processes a set of input data points using the crowd and returns the output as a single batch.
   * @param inputData and RDD containing a unique ID, input data, and crowd context for each data point.
   * @param groupContext crowd context relevant to every point in inputData
   * @return an RDD containing the processed output.
   */
  def processBlocking(inputData: RDD[(String, C)], groupContext: G, options:CrowdTaskConfiguration): RDD[(String, O)] = {
    Await.result(processStreaming(inputData, groupContext, options), Duration.Inf)
  }

  def joinResults[I: ClassTag](inputData:RDD[(String, I)], outputData:RDD[(String, O)]): RDD[(String, (I, Option[O]))] = {
    inputData.leftOuterJoin(outputData)
  }
}

/**
 * Options for setting up communication with the crowd.
 * @param crowdName crowd to send tasks to. Defaults to 'internal'
 * @param responseServerPort port on which to run the response webserver.
 * @param responseServerHost host on which to run the response webserver.
 * @param crowdServerPort port on which crowd server runs.
 * @param crowdServerHost host on which crowd server runs.
 */
case class CrowdConfiguration
(
  crowdName: String="internal",
  responseServerPort: Int=8082,
  responseServerHost: String="127.0.0.1",
  crowdServerPort: Int=8000,
  crowdServerHost: String="127.0.0.1"
  )

/**
 * Crowd task configuration. Corresponds to the crowd server's API.
 * @param maxPointsPerTask maximum number of points to batch in a single worker task.
 * @param votesPerPoint number of crowd workers to vote on a single point's result.
 * @param crowdTaskOptions custom key-value per-task settings for the current selected crowd.
 */
case class CrowdTaskConfiguration
(
  maxPointsPerTask: Int=5,
  votesPerPoint: Int=1,
  crowdTaskOptions: Map[String, Any]=Map()
  )

/**
 * An asynchronous result object that returns tuples as they are processed.
 * Callbacks are registered and executed on completion of individual or batches of tuples.
 * @tparam O type of output data
 */
class AsyncCrowdResult[O](implicit val sc: SparkContext) extends Awaitable[RDD[(String, O)]] {
  var completed = false
  var onTupleProcessedFunc: ((String, O)) => Unit = null
  var onBatchProcessedFunc: Seq[(String, O)] => Unit = null
  var batchSize = 1
  val executionPromise = Promise[Unit]()
  var results: RDD[(String, O)] = null
  var curBatch: Seq[(String, O)] = new ArrayBuffer[(String, O)]

  /** Returns all tuples processed so far. Can be called by users at any time during processing. */
  def getCurrentResults: Option[RDD[(String, O)]] = {
    Option(results)
  }

  /** Returns true if processing is complete. Can be called by users at any time during processing. */
  def isCompleted: Boolean = {
    completed
  }

  /**
   * Registers a callback to be called when processing is complete. Should be called by users.
   * To access processed data, users should call the getCurrentResults() function.
   * @param f callback that takes no arguments and returns nothing.
   */
  def onComplete(f: () => Unit): AsyncCrowdResult[O] = {
    executionPromise.future onSuccess {case _ => f()}
    this
  }

  /**
   * Registers a callback to be called when a new tuple is processed. Should be called by users.
   * @param f callback that the new tuple and returns nothing.
   */
  def onTupleProcessed(f: ((String, O)) => Unit): AsyncCrowdResult[O] = {
    onTupleProcessedFunc = f
    this
  }

  /**
   * Registers a callback to be called when a new batch of tuples is processed. Should be called by users.
   * @param f callback that the new tuples and returns nothing.
   * @param tupleBatchSize size of batches after which to call the callback.
   */
  def onBatchProcessed(f: Seq[(String, O)] => Unit, tupleBatchSize: Int = 10): AsyncCrowdResult[O] = {
    onBatchProcessedFunc = f
    batchSize = tupleBatchSize
    this
  }

  /**
   * Fires the processed tuple event.
   * This will cause the onTupleProcessed (and possibly the onBatchProcessed) callbacks to execute.
   * @param tuple the newly processed tuple.
   */
  def tupleProcessed(tuple: (String, O)) = {
    // append tuple to results
    val tupleRDD = sc.parallelize(List(tuple))
    results = if (results != null) results union tupleRDD else tupleRDD
    if (onTupleProcessedFunc != null) onTupleProcessedFunc(tuple)

    // If the current batch is full, fire the event and reset the batch.
    curBatch :+= tuple
    if (curBatch.size >= batchSize) {
      batchProcessed(curBatch)
      curBatch = new ArrayBuffer[(String, O)]()
    }
  }

  /**
   * Fires the processed batch event.
   * This will cause the onBatchProcessed callback to execute.
   * @param batch the newly processed batch of output tuples.
   */
  def batchProcessed(batch: Seq[(String, O)]) = {
    if (onBatchProcessedFunc != null) onBatchProcessedFunc(batch)
  }

  /** Fires the completed event. This will cause the onComplete callback to execute. */
  def complete(): Unit = {
    // Dispatch the last batch if it hasn't yet completed.
    if (curBatch.size >= 0) {
      batchProcessed(curBatch)
      curBatch = new ArrayBuffer[(String, O)]()
    }
    completed = true
    executionPromise success ()
  }

  // Awaitable Implementation

  /**
   * Blocks until processing is complete.
   * @see scala.concurrent.Awaitable
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): AsyncCrowdResult.this.type = {
    Await.ready(executionPromise.future, atMost)
    this
  }

  /**
   * Blocks until processing is complete, then returns the processed results.
   * @see scala.concurrent.Awaitable
   */
  override def result(atMost: Duration)(implicit permit: CanAwait): RDD[(String, O)] = {
    Await.ready(executionPromise.future, atMost)
    results
  }
}

