package sampleclean.activeml

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Class for using active selection criteria to pick points for labeling during active learning.
  * @tparam M the type of model used by the selector
  * @tparam C the point labeling context used by unlabeled points.
  */
abstract class ActivePointSelector[M, C] extends Serializable {

  /**
   * Splits points into two sets: the n points to label next, and all others.
   * @param input an RDD of unlabeled points in the form (id, feature vector, labeling context).
   * @param nPoints The number of points to select.
   * @param model A trained model.
   * @return Two RDDs in the same format as input, one consisting of points close to the margin, the other consisting
   *         of the remaining points.
   */
  def selectPoints(input: RDD[(String, Vector, C)],
                   nPoints: Int,
                   model: M): (RDD[(String, Vector, C)], RDD[(String, Vector, C)])
}

/**
  * Parameters to run the active learning framework
  * @param budget maximum number of labels to acquire during training.
  * @param batchSize number of new labels to acquire in each iteration.
  * @param bootstrapSize number of labeled points required to train the initial model.
  */
case class ActiveLearningParameters(budget: Int=10,
                                    batchSize: Int=1,
                                    bootstrapSize: Int=10)

/**
  * An algorithm that uses active learning to iteratively train models on new labeled data.
  * @tparam M model trained at each iteration
  * @tparam A parameters for the model
  * @tparam C point context for labeling new data
  * @tparam G group context for labeling new data
  * @tparam P parameters for the label getter
  */
abstract class ActiveLearningAlgorithm[M, A, C, G, P] extends Serializable {

  // TODO(dhaas): these methods would go away if we had a generic Model interface a la MLI.

  /**
    * Trains a new model instance on the available data.
    * @param data training data for the model.
    * @param parameters parameters for model training.
    * @return the trained model.
    */
  def trainModel(data: RDD[LabeledPoint], parameters:A): M

  /**
    * Uses the model to predict a point's label.
    * @param model the trained model.
    * @param point the point to predict.
    * @return the predicted label.
    */
  def predict(model:M, point: Vector): Double

  /**
   * Trains a series of models, using active learning to label new points for each new model.
   *
   * Specifically, this function:
   *   - bootstraps enough labels to train an initial model
   *   - selects a batch of new points to get labels for
   *   - gets labels for the points asynchronously by sending them to the crowd
   *   - trains a new model with each new batch of labels.
   *
   * @param labeledInput already labeled points. Each point is an
   *                     (id, [[org.apache.spark.mllib.regression.LabeledPoint]]) tuple. Pass an empty RDD if there are
   *                     no labeled points.
   * @param unlabeledInput points without labels. Each point is an (id, feature vector, labeling context) tuple.
   * @param groupContext context needed for labeling shared among all points.
   * @param algParams parameters for training individual models.
   * @param frameworkParams parameters for the active learning framework.
   * @param labelGetter label getter for adding labels to unlabeled data (e.g. using the crowd).
   * @param pointSelector point selector for picking the next points to label at each iteration.
   * @return an [[ActiveLearningTrainingFuture]], a future-like object with callbacks whenever new models are trained
   *         or new data is labeled.
   */
  def train(
    labeledInput: RDD[(String, LabeledPoint)],
    unlabeledInput: RDD[(String, Vector, C)],
    groupContext: G,
    algParams: A,
    frameworkParams: ActiveLearningParameters,
    labelGetter: LabelGetter[C, G, P],
    pointSelector: ActivePointSelector[M, C]): ActiveLearningTrainingFuture[M] = {

    // helper function that does the actual work of training.
    def runTraining(trainingState: ActiveLearningTrainingState[M]): Unit = {
      var spent = 0
      var labeledInputLocal = labeledInput
      var unlabeledInputLocal = unlabeledInput
      var numLabeled = labeledInputLocal.count()
      var numUnlabeled = unlabeledInputLocal.count()
      println(numLabeled + " labeled points given. " + numUnlabeled + " unlabeled points given.")

      // bootstrap labels if necessary
      if (numLabeled < frameworkParams.bootstrapSize) {
        val numLabelsMissing = (frameworkParams.bootstrapSize - numLabeled).toDouble
        println("not enough labeled points--bootstrapping with a random sample.")
        println(numLabelsMissing + " missing labels.")

        // split points to get the set that needs labeling
        val newLabelSplit = unlabeledInputLocal.randomSplit(Array(numLabelsMissing, numUnlabeled))
        val pointsToLabel = newLabelSplit(0)
        unlabeledInputLocal = newLabelSplit(1)
        println("split unlabeled points: " + pointsToLabel.count() + " to label, " + unlabeledInputLocal.count() + " still unlabeled")

        // get the labels
        val newLabeledPoints = labelGetter.addLabels(pointsToLabel, groupContext)
        labeledInputLocal = labeledInputLocal.union(newLabeledPoints).cache()
        numLabeled = labeledInputLocal.count()
        numUnlabeled = unlabeledInputLocal.count()
        spent += pointsToLabel.count().toInt
        println("After bootstrap: " + numLabeled + " labeled points, " + numUnlabeled + " unlabeled points.")
        println("Remaining budget: " + (frameworkParams.budget - spent))

        // update the training state with the new labeled data
        trainingState.addLabeledData(newLabeledPoints.map(p => (p._1, p._2.label)))
      }

      // train an initial model
      System.out.flush()
      println("Training initial model...")
      var model = trainModel(labeledInputLocal.map(p => p._2), algParams)
      println("Done. Train Error=" + trainError(model, labeledInputLocal.map(p => p._2), numLabeled))

      // update the training state with the new model
      trainingState.addModel(model, numLabeled)

      while (spent < frameworkParams.budget) {
        // decide on the next points to label
        var batchSize = math.min(frameworkParams.batchSize, frameworkParams.budget - spent)
        println("Getting labels for " + batchSize + " new points...")
        val nextPoints = pointSelector.selectPoints(unlabeledInputLocal, batchSize, model)

        // get the labels
        val newLabeledPoints = labelGetter.addLabels(nextPoints._1, groupContext)
        labeledInputLocal = labeledInputLocal.union(newLabeledPoints).cache()
        unlabeledInputLocal = nextPoints._2
        numLabeled = labeledInputLocal.count()
        numUnlabeled = unlabeledInputLocal.count()
        spent += batchSize
        println("Done. Now " + numLabeled + " labeled points, " + numUnlabeled + " unlabeled points.")

        // update the training state with the new labeled data
        trainingState.addLabeledData(newLabeledPoints.map(p => (p._1, p._2.label)))

        // retrain the model
        println("Retraining model...")
        model = trainModel(labeledInputLocal.map(p => p._2), algParams)
        println("Done. Train Error=" + trainError(model, labeledInputLocal.map(p => p._2), numLabeled))
        println(frameworkParams.budget - spent + " labels left in budget.")

        // update the training state with the new model
        trainingState.addModel(model, numLabeled)
      }

      // clean up
      labelGetter.cleanUp()

    }

    // return the asynchronous model training context
    new ActiveLearningTrainingFuture[M](runTraining)
  }


  /**
   * Trains a series of models, using uncertainty sampling to label new points for each new model.
   *
   * Specifically, this function:
   *   - bootstraps enough labels to train an initial model
   *   - selects a batch of new points to get labels for using uncertainty sampling
   *   - gets labels for the points asynchronously by sending them to the crowd
   *   - trains a new model with each new batch of labels.
   *
   * Uses default values for active learning framework parameters.
   *
   * @param labeledInput already labeled points. Each point is an
   *                     (id, [[org.apache.spark.mllib.regression.LabeledPoint]]) tuple. Pass an empty RDD if there are
   *                     no labeled points.
   * @param unlabeledInput points without labels. Each point is an (id, feature vector, labeling context) tuple.
   * @param groupContext context needed for labeling shared among all points.
   * @param algParams parameters for training individual models.
   * @param labelGetter label getter for adding labels to unlabeled data (e.g. using the crowd).
   * @param pointSelector point selector for picking the next points to label at each iteration.
   * @return an [[ActiveLearningTrainingFuture]], a future-like object with callbacks whenever new models are trained
   *         or new data is labeled.
   */
  def train(labeledInput: RDD[(String, LabeledPoint)],
            unlabeledInput: RDD[(String, Vector, C)],
            groupContext: G,
            algParams: A,
            labelGetter: LabelGetter[C, G, P],
            pointSelector: ActivePointSelector[M, C]): ActiveLearningTrainingFuture[M] = {
    train(labeledInput, unlabeledInput, groupContext, algParams, new ActiveLearningParameters(),
      labelGetter, pointSelector)
  }

  /**
    * Classification Error on a training set.
    * @param model a trained model.
    * @param trainingData an RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]s used to train the model.
    * @param trainN the size of trainingData.
    * @return the fraction of training examples incorrectly classified by the model.
    */
  def trainError(model: M, trainingData: RDD[LabeledPoint], trainN: Long): Double = {
    val labelAndPreds = trainingData.map { point =>
      val prediction = predict(model, point.features)
      (point.label, prediction)
    }
    labelAndPreds.filter(r => r._1 != r._2).count().toDouble / trainN
  }

}
