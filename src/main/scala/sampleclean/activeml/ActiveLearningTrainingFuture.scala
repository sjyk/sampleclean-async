package sampleclean.activeml

import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.util.{Failure, Success}

import ExecutionContext.Implicits.global

/**
  * Future-like class for asynchronously passing back models and labeled data output by the active learning framework.
  *
  * The training code should be passed in as trainFunc, which should be a function that takes an
  * ActiveLearningTrainingState object and calls addModel() and addLabeledData on it whenever new models and data are
  * available. Example:
  * {{{
  * def train(trainingState: ActiveLearningTrainingState[M]) = {
  *   // get new labels
  *   newLabeledData = labelSomeData()
  *
  *   // register them with the training state.
  *   trainingState.addLabeledData(newLabeledData)
  *
  *   // train a model and register it
  *   model = mllib.classification.SVMWithSGD.train(...)
  *   trainingState.addModel(model)
  *
  *   // ... iterate on data and models ...
  * }
  * }}}
  *
  * This object will be returned to users training models. Users should use the onNewModel, onNewLabeledData,
  * isCompleted, and onComplete callbacks to process state as the training progresses. The full history of trained models
  * and labeled data can be fetched with getModels and getLabeledData, respectively. This class has the Awaitable trait,
  * so users can block until training is complete with Await.ready or Await.result. Example:
  * {{{
  * val trainingFuture = ActiveLearningFramework.train(...)
  *
  * // add callbacks for new models and new data.
  * trainingFuture.onNewModel(evaluateNewModel)
  * trainingFuture.onNewLabeledData(processNewLabeledData)
  * trainingFuture.onComplete(() => println("All done! " + trainingFuture.getModels.size + " models trained."))
  *
  * // wait for training to complete
  * Await.ready(trainingFuture, Duration.Inf)
  * }}}
  *
  * @constructor create a new future that runs an active learning training function.
  * @tparam M the type of the Model being trained.
  * @param trainFunc the function that actually runs the training loop.
  *
  */
class ActiveLearningTrainingFuture[M](trainFunc: ActiveLearningTrainingState[M] => Unit) extends Awaitable[Unit]{
  var completed = false
  var onCompleteFunc: () => Unit = null
  var onNewModelFunc: (M, Long) => Unit = null
  var onNewLabeledDataFunc: RDD[(String, Double)] => Unit = null
  val state = new ActiveLearningTrainingState[M](this)

  // a Future to execute the training function asynchronously. We don't actually use its result, which is of type Unit.
  val executionFuture: Future[Unit] = Future {
    trainFunc(state)
  }
  executionFuture onComplete {
    case Success(s) => complete()
    case Failure(t) => throw t
  }

  /** Returns all models trained so far. Can be called by users at any time during training. */
  def getModels: Seq[(M, Long)] = {
    state.models
  }

  /** Returns all data points labeled so far. Can be called by users at any time during training. */
  def getLabeledData: Option[RDD[(String, Double)]] = {
    Option(state.labeledData)
  }

  /** Returns true if the training process is complete. Can be called by users at any time during training. */
  def isCompleted: Boolean = {
    completed
  }

  /**
    * Registers a callback to be called when the training process is complete. Should be called by users.
    * @param f callback that takes no arguments and returns nothing.
   */
  def onComplete(f: () => Unit): Unit = {
    onCompleteFunc = f
  }

  /**
    * Registers a callback to be called when new models are trained. Should be called by users.
    * @param f callback that takes a trained model and the size of that model's training set and returns nothing.
    */
  def onNewModel(f: (M, Long) => Unit): Unit = {
    onNewModelFunc = f
  }

  /**
    * Registers a callback to be called when new data has been labeled. Should be called by users.
    * @param f callback that takes an RDD of newly labeled points of the form (id, label) and returns nothing.
    */
  def onNewLabeledData(f: RDD[(String, Double)] => Unit): Unit = {
    onNewLabeledDataFunc = f
  }

  /**
    * Fires the new model event. This will cause the onNewModel callback to execute.
    * @param model the newly trained model.
    * @param trainN the size of the model's training set.
    */
  def newModel(model: M, trainN: Long) = {
    if (onNewModelFunc != null) {
      onNewModelFunc(model, trainN)
    }
  }

  /**
    * Fires the new labeled data event. This will cause the onNewLabeledData callback to execute.
    * @param data an RDD of newly labeled data of the form (id, label).
    */
  def newLabeledData(data: RDD[(String, Double)]) = {
    if (onNewLabeledDataFunc != null) {
      onNewLabeledDataFunc(data)
    }
  }

  /** Fires the completed event. This will cause the onComplete callback to execute. */
  def complete(): Unit = {
    completed = true
    if (onCompleteFunc != null) {
      onCompleteFunc()
    }
  }

  // Awaitable Implementation

  /**
    * Blocks until active learning is complete.
    * @see scala.concurrent.Awaitable
    */
  override def ready(atMost: Duration)(implicit permit: CanAwait): ActiveLearningTrainingFuture.this.type = {
    Await.ready(executionFuture, atMost)
    this
  }

  /**
    * Blocks until active learning is complete. Returns nothing since there is no result.
    * @see scala.concurrent.Awaitable
    */
  override def result(atMost: Duration)(implicit permit: CanAwait): Unit = {
    Await.ready(executionFuture, atMost)
  }
}