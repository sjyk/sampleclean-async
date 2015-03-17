package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.mllib.classification.SVMModel
import sampleclean.clean.featurize.Tokenizer.NullTokenizer
import sampleclean.clean.deduplication.ActiveLearningStrategy
import sampleclean.api.SampleCleanContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

/* This class implements the similarity based featurizer used in Deduplication
 */
@serializable
class LearningSimilarityFeaturizer(colNames: List[String],
                  context: List[String],
                  val baseFeaturizer: SimilarityFeaturizer,
                  scc: SampleCleanContext,
                  val activeLearningStrategy: ActiveLearningStrategy,
								  threshold:Double=0,
                  minSize: Int = 0,
                  schemaMap: Map[Int,Int]= null)
	extends AnnotatedSimilarityFeaturizer(colNames, context, new NullTokenizer(), threshold, minSize,schemaMap){

    val canPrefixFilter = false

    val colMapper = (colSubSet: List[String]) => colSubSet.map(x => cols(colNames.indexOf(x)))

    def similarity(tokens1:Seq[String], 
                   tokens2:Seq[String], 
                   thresh:Double,
                   tokenWeights: collection.Map[String, Double]): (Boolean,Double) =
    {
        val featureVector = baseFeaturizer.getSimilarities(tokens1(0),tokens2(0))
        return (activeLearningStrategy.currentModel.predict(Vectors.dense(featureVector.toArray)) > 0.5, 
                activeLearningStrategy.currentModel.predict(Vectors.dense(featureVector.toArray)))
    }

    def train(trainingPairs:RDD[(Row,Row)]) = {

        val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
        activeLearningStrategy.asyncRun(emptyLabeledRDD, 
                                        trainingPairs, 
                                        colMapper, 
                                        colMapper, 
                                        this.updateModel(_))

    }

    def  updateModel(trainingPairs:RDD[(Row,Row)]) = {
      println("[SampleClean] Model Updated")
    }

}