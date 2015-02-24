package sampleclean.clean.deduplication

import sampleclean.api.SampleCleanContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import sampleclean.clean.algorithm.SampleCleanDeduplicationAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}
import sampleclean.activeml._
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.graphx._
import sampleclean.crowd._
import sampleclean.crowd.context.{DeduplicationPointLabelingContext, DeduplicationGroupLabelingContext}

import sampleclean.simjoin.SimilarityJoin
import sampleclean.clean.featurize.BlockingFeaturizer


class ALRecordDeduplication(params:AlgorithmParameters,
                            scc: SampleCleanContext) extends
                            AbstractRecordDeduplication(params, scc) {

  def exec(sampleTableName: String) = {
    val idCol = params.get("id").asInstanceOf[String]

    /* Blocking stage */
    val sampleTableRDD = scc.getCleanSample(sampleTableName)
    val fullTableRDD = scc.getFullTable(sampleTableName)
    val sampleTableColMapper = scc.getSampleTableColMapper(sampleTableName)
    val fullTableColMapper = scc.getFullTableColMapper(sampleTableName)

    val blockingFeaturizer = params.get("blockingFeaturizer").asInstanceOf[BlockingFeaturizer]
    //TODO: projection
    val fullSchema = (0 to fullTableRDD.schema.fields.size).toList
    val fullSchema2 = scc.getHiveTableSchema(sampleTableName).size
    val candidatePairs = blockingJoin(sampleTableRDD,fullTableRDD,blockingFeaturizer, fullSchema,true,true,true, "BroadcastJoin")

    val sampleCol = scc.getColAsIndex(sampleTableName, idCol)
    val fullTableCol = scc.getColAsIndexFromBaseTable(sampleTableName, idCol)

    // Remove those pairs that have the same id from candidatePairs because they must be duplicate
    val shrinkedCandidatePairs = candidatePairs.filter{ case (fullRow, sampleRow) =>
      fullRow(fullTableCol).asInstanceOf[String] != sampleRow(sampleCol).asInstanceOf[String]
    }

    if (!params.exist("activeLearningStrategy")){
      // machine-only deduplication
      onReceiveCandidatePairs(shrinkedCandidatePairs,sampleTableName)
    }
    else{
      // Refine candidate pairs using ActiveCrowd
      val emptyLabeledRDD = scc.getSparkContext().parallelize(new Array[(String, LabeledPoint)](0))
      val activeLearningStrategy = params.get("activeLearningStrategy").asInstanceOf[ActiveLearningStrategy]

      activeLearningStrategy.asyncRun(emptyLabeledRDD,
                                      shrinkedCandidatePairs,
                                      fullTableColMapper,
                                      sampleTableColMapper,
                                      onReceiveCandidatePairs(_, sampleTableName))
    }

  }

}
