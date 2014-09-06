package sampleclean.activeml

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * A generic label-getter for adding labels to unlabeled feature vectors.
 * @tparam C type of the point context for label acquisition.
 * @tparam G type of the group context for label acquisition.
 * @tparam P type of the parameters for this class.
 * @param parameters parameters for this class.
 */
abstract class LabelGetter[C, G, P] (parameters: P) extends Serializable {
  /**
   * Adds labels to an RDD of unlabeled points. Subclasses should override.
   * @param points an RDD of unlabeled points (id, feature vector, point context).
   * @param groupContext group labeling context shared by all points.
   * @return an RDD of labeled points (id, feature vector)
   */
  def addLabels(points:RDD[(String, Vector, C)], groupContext:G): RDD[(String, LabeledPoint)]

  /** Does any necessary cleanup once the labelGetter is done being used. */
  def cleanUp()
}
