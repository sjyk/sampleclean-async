package sampleclean.activeml

import breeze.stats.MeanAndVariance
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model.{Node, DecisionTreeModel, RandomForestModel}
import sampleclean.crowd.context.{GroupLabelingContext, PointLabelingContext}

case class RandomForestParameters(numTrees: Int=100,
                                  numClasses: Int=2,
                                  categoricalFeatureInfo: Map[Int, Int]=Map(),
                                  impurity: String="gini",
                                  maxDepth: Int=4,
                                  maxBins: Int=100,
                                  featureSubsetStrategy: String="auto",
                                  algo: Algo=Algo.Classification)

class UncertaintySamplingRandomForest[C <: PointLabelingContext, G <: GroupLabelingContext] extends ActiveLearningAlgorithm[RandomForestModel, RandomForestParameters, C, G] {

  /**
   * Trains a new model instance on the available data.
   * @param data training data for the model.
   * @param parameters parameters for model training.
   * @return the trained model.
   */
  override def trainModel(data: RDD[LabeledPoint], parameters: RandomForestParameters): RandomForestModel = {
    parameters.algo match {
      case Algo.Classification => RandomForest.trainClassifier(data, parameters.numClasses,
        parameters.categoricalFeatureInfo, parameters.numTrees,
        parameters.featureSubsetStrategy, parameters.impurity, parameters.maxDepth, parameters.maxBins)
      case Algo.Regression => RandomForest.trainRegressor(data, parameters.categoricalFeatureInfo, parameters.numTrees,
        parameters.featureSubsetStrategy, parameters.impurity, parameters.maxDepth, parameters.maxBins)
    }
  }

  /**
   * Uses the model to predict a point's label.
   * @param model the trained model.
   * @param point the point to predict.
   * @return the predicted label.
   */
  override def predict(model: RandomForestModel, point: Vector): Double = model.predict(point)

  def extractPaths(model: RandomForestModel): Seq[TreePath] = {
    model.trees flatMap { tree =>
      enumerateLeaves(tree, first = true) map {leaf => new TreePath(tree.topNode, leaf)} filter { path =>
        path.prediction == -1.0
      }
    }
  }

  def enumerateLeaves(tree: DecisionTreeModel, startNode: Node = null, first:Boolean = false): Seq[Node] = {
    val curNode = if (first) tree.topNode else startNode
    if (curNode == null) List[Node]()
    if (curNode.isLeaf) List(curNode) else {
      enumerateLeaves(tree, curNode.leftNode.getOrElse(null)) ++ enumerateLeaves(tree, curNode.rightNode.getOrElse(null))
    }
  }
}

/** Uses uncertainty sampling to select points closest to the SVM Margin. */
class ForestUncertaintyFilter[C <: PointLabelingContext] extends ActivePointSelector[RandomForestModel, C] {

  /**
   * Splits points into two sets: the n that the random forest is least certain about, and all others.
   * Uncertainty is simply calculated using variance among the trees in the forest
   * @param input an RDD of unlabeled points in the form (id, feature vector, labeling context).
   * @param nPoints The number of points to select.
   * @param model A RandomForest model with trained weights.
   * @return Two RDDs in the same format as input, one consisting of points close to the margin, the other consisting
   *         of the remaining points.
   */
  def selectPoints(input: RDD[(String, Vector, C)],
                   nPoints: Int,
                   model: RandomForestModel): (RDD[(String, Vector, C)], RDD[(String, Vector, C)]) = {
    val withUncertainty = input.map { point =>
      val treePredictions: Seq[Double] = model.trees.map(_.predict(point._2))
      val meanAndVariance:MeanAndVariance = breeze.stats.meanAndVariance(treePredictions)
      (meanAndVariance.variance, point)
    }
    val cutoff = withUncertainty.sortByKey(ascending = false).map(p => p._1).take(nPoints).last
    if (cutoff == 0 ) { // model predicts only one class, just select random points
      val withRandomIDs = input.map { point=> (utils.randomUUID(), point)}.cache()
      val selectedWithIds = withRandomIDs.takeSample(withReplacement = false, num = nPoints)
      val selectedIds = Set(selectedWithIds map (_._1): _*)
      val selected = input.sparkContext.parallelize(selectedWithIds.map(_._2))
      val unselected = withRandomIDs filter { point => selectedIds contains point._1} map { point => point._2}
      (selected, unselected)
    } else {
      val selected = withUncertainty.filter(p => p._1 >= cutoff).map(p => p._2)
      val unselected = withUncertainty.filter(p => p._1 < cutoff).map(p => p._2)
      (selected, unselected)
    }
  }
}
