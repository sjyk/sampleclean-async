package sampleclean.activeml

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import sampleclean.crowd.{CrowdLabelGetterParameters, GroupLabelingContext, PointLabelingContext}

/**
 * Parameters to train the SVMWithSGD model
 * @param numIterations number of iterations of gradient descent to run.
 * @param stepSize step size to be used for each iteration of gradient descent.
 * @param regParam regularization parameter.
 * @param miniBatchFraction fraction of data to be used per iteration.
 * @see org.apache.spark.mllib.classification.SVMWithSGD
 */
case class SVMParameters(numIterations: Int=100,
                         stepSize: Double=1.0,
                         regParam: Double=1.0,
                         miniBatchFraction: Double=1.0)


/** Active learning to train an SVM with SGD. */
object ActiveSVMWithSGD extends ActiveLearningAlgorithm[SVMModel, SVMParameters, PointLabelingContext,
  GroupLabelingContext, CrowdLabelGetterParameters] {

  /**
    * Trains a new model instance on the available data.
    * @param data training data for the model.
    * @param parameters parameters for model training.
    * @return the trained model.
    */
  override def trainModel(data: RDD[LabeledPoint], parameters: SVMParameters): SVMModel = {
    SVMWithSGD.train(data, parameters.numIterations, parameters.stepSize, parameters.regParam,
      parameters.miniBatchFraction)
  }

  /**
    * Uses the model to predict a point's label.
    * @param model the trained model.
    * @param point the point to predict.
    * @return the predicted label.
    */
  override def predict(model: SVMModel, point: Vector): Double = model.predict(point)
}

/** Uses uncertainty sampling to select points closest to the SVM Margin. */
object SVMMarginDistanceFilter extends ActivePointSelector[SVMModel, PointLabelingContext] {

  /**
    * Splits points into two sets: the n closest to the SVM's margin, and all others.
    * @param input an RDD of unlabeled points in the form (id, feature vector, labeling context).
    * @param nPoints The number of points to select.
    * @param model An SVM model with trained weights.
    * @return Two RDDs in the same format as input, one consisting of points close to the margin, the other consisting
    *         of the remaining points.
    */
  def selectPoints(input: RDD[(String, Vector, PointLabelingContext)],
                    nPoints: Int,
                    model: SVMModel): (RDD[(String, Vector, PointLabelingContext)], RDD[(String, Vector, PointLabelingContext)]) = {
    val breezeWeights = new BDV[Double](model.weights.toArray)
    val withMargins : RDD[(Double, (String, Vector, PointLabelingContext))] = input.map(point => {
      val margin : Double = breezeWeights.dot(new BDV[Double](point._2.toArray))
      (math.abs(margin), point)
    })
    val cutoff = withMargins.sortByKey().map(p => p._1).take(nPoints).last
    val selected = withMargins.filter(p => p._1 <= cutoff).map(p => p._2)
    val unselected = withMargins.filter(p => p._1 > cutoff).map(p => p._2)
    (selected, unselected)
  }
}