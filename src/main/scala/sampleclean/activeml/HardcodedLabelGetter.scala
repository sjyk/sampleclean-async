package sampleclean.activeml

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Parameters for the hardcoded label getter.
  * @param hardcodedLabeledData already labeled data.
  */
case class HardcodedLabelGetterParameters(hardcodedLabeledData: RDD[(String, LabeledPoint)])

/** Unused context for labeling points. */
case class HardcodedLabelGetterPointContext()

/** Unused shared context for labeling groups of points. */
case class HardcodedLabelGetterGroupContext()

/**
  * Dummy label getter that is initialized with labeled data and simply returns the labels when queried.
  * @constructor stores the already-labeled data for later retrieval.
  * @param parameters parameters for the label getter including the pre-labeled data.
  */
class HardCodedLabelGetter(parameters: HardcodedLabelGetterParameters)
  extends LabelGetter[HardcodedLabelGetterPointContext, HardcodedLabelGetterGroupContext, HardcodedLabelGetterParameters](parameters) {
  private val labelMap = Map(parameters.hardcodedLabeledData.map(p => p._1 -> p._2.label).collect() : _*)

  /**
    * Add previously-provided labels to an RDD of unlabeled points
    * @param points an RDD of unlabeled points (id, feature vector, point context).
    * @param groupContext group labeling context shared by all points.
    * @return an RDD of labeled points (id, feature vector)
    */
  override def addLabels(points: RDD[(String, Vector, HardcodedLabelGetterPointContext)], groupContext:HardcodedLabelGetterGroupContext): RDD[(String, LabeledPoint)] = {
    points.map(p => p._1 -> new LabeledPoint(labelMap(p._1), p._2))
  }

  /** do any necessary cleanup once the labelGetter is done being used. */
  override def cleanUp(): Unit = {}
}