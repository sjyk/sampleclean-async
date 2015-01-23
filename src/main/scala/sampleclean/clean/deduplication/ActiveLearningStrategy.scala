package sampleclean.clean.deduplication


import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd._
import org.apache.spark.sql._
import sampleclean.activeml.{ActiveLearningParameters, SVMParameters, _}
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationGroupLabelingContext, DeduplicationPointLabelingContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * This class is used to create an Active Learning strategy that will
 * asynchronously run an Active Learning algorithm ultimately used
 * fot deduplication. It uses given starting
 * labels and Amazon Mechanical Turk for training new models.
 * @param displayedColNames column names of the main data set (i.e. that are visible to the user).
 */
case class ActiveLearningStrategy(displayedColNames: List[String]) {

  var featureList: List[Feature] = displayedColNames.map(col => Feature(List(col), List("JaroWinkler", "JaccardSimilarity")))
  var svmParameters = SVMParameters()
  var frameworkParameters = ActiveLearningParameters()
  var crowdParameters = CrowdConfiguration() // Use defaults
  var taskParameters = CrowdTaskConfiguration() // Use defaults

  /**
   * Used to set a new Feature list to be used for training.
   * @param featureList list of Features (not Feature Vector)
   */
  def setFeatureList(featureList: List[Feature]): ActiveLearningStrategy = {
    this.featureList = featureList
    this
  }

  /** get current Feature list.*/
  def getFeatureList: List[Feature] = {
    this.featureList
  }

  /**
   * Used to set new SVM parameters that will be used for training.
   * @param svmParameters parameters to set.
   */
  def setSVMParameters(svmParameters: SVMParameters): ActiveLearningStrategy = {
    this.svmParameters = svmParameters
    this
  }

  /** get current SVM parameters.*/
  def getSVMParameters: SVMParameters = {
    this.svmParameters
  }

  /**
   * Used to set new Active Learning framework parameters that will be used for training.
   * @param frameworkParameters parameters to set.
   */
  def setActiveLearningParameters(frameworkParameters: ActiveLearningParameters): ActiveLearningStrategy = {
    this.frameworkParameters = frameworkParameters
    this
  }

  /** get current framework parameters.*/
  def getActiveLearningParameters: ActiveLearningParameters = {
    this.frameworkParameters
  }

  /**
   * Used to set new crowd parameters that will be used for training.
   * @param crowdParams parameters to set.
   */
  def setCrowdParameters(crowdParams: CrowdConfiguration): ActiveLearningStrategy = {
    this.crowdParameters = crowdParams
    this
  }

  /** get current Label-Getter parameters.*/
  def getCrowdParameters: CrowdConfiguration = {
    this.crowdParameters
  }

  /**
   * Used to set new crowd task parameters that will be used for training.
   * @param crowdTaskParams parameters to set.
   */
  def setTaskParameters(crowdTaskParams: CrowdTaskConfiguration): ActiveLearningStrategy = {
    this.taskParameters = crowdTaskParams
    this
  }

  /** get current Label-Getter parameters.*/
  def getTaskParameters: CrowdTaskConfiguration = {
    this.taskParameters
  }

  /**
   * This method is the main executor of the Active Learning Strategy.
   *
   * @param labeledInput initial labels used for training. An empty RDD is valid.
   * @param candidatePairs Pairs that will be compared using the crowd.
   *                       The two data sets being compared can have different column schemas.
   * @param colMapper1 function that converts a list of column names
   *                   in the first schema into a list of those columns' indices.
   * @param colMapper2 function that converts a list of column names
   *                   in the second schema into a list of those columns' indices.
   * @param onUpdateDupCounts link to SampleClean that will update the sample table after the
   *                          Active Learning algorithm.
   */
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

    /**
     * Defines parameters for labeling a entity resolution task
     * @param row1 row for first record
     * @param row2 row for second record
     */
    def toPointLabelingContext(row1: Row, row2: Row): DeduplicationPointLabelingContext = {

      val displayedRow1 = displayedColIndices1.map(row1(_).toString).toList
      val displayedRow2 = displayedColIndices2.map(row2(_).toString).toList

      DeduplicationPointLabelingContext(List(displayedRow1, displayedRow2))
    }

    val unlabeledInput = candidatePairsWithId.map(p =>
      (p._1, Vectors.dense(featureVector.toFeatureVector(p._2._1, p._2._2)), toPointLabelingContext(p._2._1, p._2._2)))

    // Render Context for a deduplication task
    val groupLabelingContext = DeduplicationGroupLabelingContext(
      "er", Map("fields" -> displayedColNames.toList))


    // Start training models
    val crowdTask = new DeduplicationTask()
    crowdTask.configureCrowd(getCrowdParameters)
    val activeLearningAlgorithm = new ActiveSVMWithSGD[DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext]()
    val trainingFuture = activeLearningAlgorithm.train(
      labeledInput,
      unlabeledInput,
      groupLabelingContext,
      getSVMParameters,
      getActiveLearningParameters,
      crowdTask,
      getTaskParameters,
      new SVMMarginDistanceFilter[DeduplicationPointLabelingContext]())

    /**
     * Process new Active Learning model and update tables in the
     * SampleClean context accordingly.
     * @param model SVM model.
     * @param modelN size of the model's training set.
     */
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
