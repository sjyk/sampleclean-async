package sampleclean.clean.deduplication


import scala.List
import sampleclean.activeml._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import sampleclean.activeml.SVMParameters
import scala.Some
import sampleclean.activeml.DeduplicationGroupLabelingContext
import sampleclean.activeml.DeduplicationPointLabelingContext
import sampleclean.activeml.ActiveLearningParameters
import org.apache.spark.mllib.regression.LabeledPoint
import sampleclean.activeml.CrowdLabelGetterParameters


case class ActiveLearningStrategy(displayedColNames: List[String]) {

  var featureList: List[Feature] = displayedColNames.map(col => Feature(List(col), List("JaroWinkler", "JaccardSimilarity")))
  var svmParameters = SVMParameters()
  var frameworkParameters = ActiveLearningParameters()
  var labelGetterParameters = CrowdLabelGetterParameters() // Use defaults


  def setFeatureList(featureList: List[Feature]): ActiveLearningStrategy = {
    this.featureList = featureList
    return this
  }
  def getFeatureList(): List[Feature] = {
    return this.featureList
  }

  def setSVMParameters(svmParameters: SVMParameters): ActiveLearningStrategy = {
    this.svmParameters = svmParameters
    return this
  }
  def getSVMParameters(): SVMParameters = {
    return this.svmParameters
  }

  def setActiveLearningParameters(frameworkParameters: ActiveLearningParameters): ActiveLearningStrategy = {
    this.frameworkParameters = frameworkParameters
    return this
  }
  def getActiveLearningParameters(): ActiveLearningParameters = {
    return this.frameworkParameters
  }

  def setCrowdLabelGetterParameters(labelGetterParameters: CrowdLabelGetterParameters): ActiveLearningStrategy = {
    this.labelGetterParameters = labelGetterParameters
    return this
  }
  def getCrowdLabelGetterParameters(): CrowdLabelGetterParameters = {
    return this.labelGetterParameters
  }


  def asyncRun(labeledInput: RDD[(String, LabeledPoint)],
               candidatePairs: RDD[(Row, Row)],
               colMapper1: List[String] => List[Int],
               colMapper2: List[String] => List[Int],
               onUpdateDupCounts: RDD[(Row, Row)] => Unit) = {

    // Assign a unique id for each candidate pair
    val candidatePairsWithId = candidatePairs.map((utils.randomUUID(), _)).cache()

    // Construct unlabeled input
    val featureVector = FeatureVector(featureList, colMapper1, colMapper2)
    val displayedColIndices1 = colMapper1(displayedColNames)
    val displayedColIndices2 = colMapper2(displayedColNames)
    def toPointLabelingContext(row1: Row, row2: Row): PointLabelingContext = {
      val displayedRow1 = displayedColIndices1.map(row1.getString(_)).toList
      val displayedRow2 = displayedColIndices2.map(row2.getString(_)).toList

      DeduplicationPointLabelingContext(List(displayedRow1, displayedRow2))
    }
    val unlabeledInput = candidatePairsWithId.map(p =>
      (p._1, Vectors.dense(featureVector.toFeatureVector(p._2._1, p._2._2)), toPointLabelingContext(p._2._1, p._2._2)))

    // Render Context for a deduplication task
    val groupLabelingContext = DeduplicationGroupLabelingContext(
      "er", Map("fields" -> displayedColNames.toList)).asInstanceOf[GroupLabelingContext]


    val trainingFuture = ActiveSVMWithSGD.train(
      labeledInput,
      unlabeledInput,
      groupLabelingContext,
      svmParameters,
      frameworkParameters,
      new CrowdLabelGetter(labelGetterParameters),
      SVMMarginDistanceFilter)

    def processNewModel(model:SVMModel, modelN: Long) {
      val modelLabeledData: RDD[(String, Double)] = unlabeledInput.map(p => (p._1, model.predict(p._2)))
      var mergedLabeledData: RDD[(String, Double)] = modelLabeledData

      val crowdLabeledData = trainingFuture.getLabeledData
      crowdLabeledData match {
        case None => // do nothing
        case Some(crowdData) =>
          mergedLabeledData = modelLabeledData.leftOuterJoin(crowdData).map{
            case (pid, (modelLabel, None)) => (pid, modelLabel)
            case (pid, (modelLabel, Some(crowdLabel))) => (pid, crowdLabel)
        }
      }

      assert(mergedLabeledData.count() == modelLabeledData.count())
      assert(mergedLabeledData.count() == candidatePairsWithId.count())

      val duplicatePairs = mergedLabeledData.filter(_._2 > 0.5).join(candidatePairsWithId).map(_._2._2) // 1: duplicate; 0: non-duplicate
      onUpdateDupCounts(duplicatePairs)
    }

    trainingFuture.onNewModel(processNewModel)

    // wait for training to complete
    Await.ready(trainingFuture, Duration.Inf)
  }


}
