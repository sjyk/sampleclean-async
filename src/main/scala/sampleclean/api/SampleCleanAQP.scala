package sampleclean.api

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import sampleclean.util.TypeUtils._
import sampleclean.util.QueryBuilder._

/* This class provides the approximate query processing 
* for SampleClean. Currently, it supports SUM, COUNT, AVG
* and returns confidence intervals in the form of CLT variance
* estimates.
*/
@serializable
class SampleCleanAQP() {

	  /**This function executes the per-partition query processing of the agg function
	   * Notice that this operation returns a single tuple from each partition which is
	     then aggregated in a reduce call.
	   */
	  private def aqpPartitionAgg(partitionData:Iterator[Double]): Iterator[(Double,Double,Double)] =
	  {
	  		var result = 0.0
	  		var variance = 0.0
	  		var n = 0.0

	  		for(tuple <- partitionData)
	  		{
	  			n = n + 1
	  			result = result*((n-1)/n) + tuple/n
	  			val dev = Math.pow((tuple - result),2)
	  			variance = variance*((n-1)/n) + dev/n
	  		}

	  		return List((result,variance, n)).iterator
	  }

	  /**Helper function that "transforms" our queries into mean queries
	  */
	  private def aqpPartitionMap(row:Row, transform: Double => Double): Double = 
	  {
	  		return transform(rowToNumber(row,0))/rowToNumber(row,1)
	  }

	  /**Internal method to execute the count query, returns a tuple of (Result, Confidence)
	  */
	  private def approxCount(rdd:SchemaRDD, sampleRatio:Double):(Double, Double)=
	  {

	  	  val partitionResults = rdd.coalesce(4,true).map(row => aqpPartitionMap(row,x => x))
	  	  							.mapPartitions(aqpPartitionAgg, true).collect()
	  	  var count:Double = 0.0
	  	  var variance:Double = 0.0
	  	  var emptyPartitions = 0
	  	  var k = 0.0

	  	  for(p <- partitionResults)
	  	  {
	  	  	count = count + p._1.asInstanceOf[Double]
	  	  	variance = variance + p._2.asInstanceOf[Double]
	  	  	
	  	  	if (p._3.asInstanceOf[Double] == 0.0)
	  	  		emptyPartitions = emptyPartitions + 1

	  	  }
	  	  val splitSize = partitionResults.length - emptyPartitions
	  	  return (rdd.count()*count/(splitSize*sampleRatio),
	  	  	      (rdd.count()/sampleRatio)*Math.sqrt(variance/splitSize)/
	  	  	       Math.sqrt(rdd.count()))
	  }


	  /**Internal method to execute the sum query, returns a tuple of (Result, Confidence)
	  */
	  private def approxSum(rdd:SchemaRDD, sampleRatio:Double):(Double, Double)=
	  {

	  	  val partitionResults = rdd.coalesce(4,true).map(row => aqpPartitionMap(row,x => x))
	  	  							.mapPartitions(aqpPartitionAgg, true).collect()
	  	  var sum:Double = 0.0
	  	  var variance:Double = 0.0
	  	  var emptyPartitions = 0
	  	  for(p <- partitionResults)
	  	  {
	  	  	sum = sum + p._1.asInstanceOf[Double]
	  	  	variance = variance + p._2.asInstanceOf[Double]

	  	  	if (p._3.asInstanceOf[Double] == 0.0)
	  	  		emptyPartitions = emptyPartitions + 1
	  	  }
	  	  val splitSize = partitionResults.length - emptyPartitions
	  	  return (rdd.count()*sum/(splitSize*sampleRatio),
	  	  	       (rdd.count()/sampleRatio)*Math.sqrt(variance/splitSize)/
	  	  	       Math.sqrt(rdd.count()))
	  }

	 /**Internal method to execute the AVG query, returns a tuple of (Result, Confidence)
	  */
	  private def approxAvg(rdd:SchemaRDD, sampleRatio:Double):(Double, Double)=
	  {
	  	  val partitionResults = rdd.coalesce(4,true).map(row => aqpPartitionMap(row,x => x))//todo fix
	  	  							.mapPartitions(aqpPartitionAgg, true).collect()
	  	  var sum:Double = 0.0
	  	  var variance:Double = 0.0
	  	  var emptyPartitions = 0
	  	  for(p <- partitionResults)
	  	  {
	  	  	sum = sum + p._1.asInstanceOf[Double]
	  	  	variance = variance + p._2.asInstanceOf[Double]
	  	  	if (p._3.asInstanceOf[Double] == 0.0)
	  	  		emptyPartitions = emptyPartitions + 1
	  	  }
	  	  val splitSize = partitionResults.length - emptyPartitions
	  	  return (duplicationRate(rdd)*sum/(splitSize)
	  	  	     ,duplicationRate(rdd)*Math.sqrt(variance/splitSize)/
	  	  	     Math.sqrt(rdd.count()))
	  }

	  /**Internal method to calculate the normalization for the AVG query
	  */
	  private def duplicationRate(rdd:SchemaRDD):Double=
	  {
	  	  return rdd.count()/rdd.map( x => 1.0/x(1).asInstanceOf[Int]).reduce(_ + _)
	  }

	  /*This query executes rawSC given an attribute to aggregate, expr {SUM, COUNT, AVG}, a predicate, and the sampling ratio.
	  * It returns a tuple of the estimate, and the variance of the estimate (EST, VAR_EST)

	  *Args (SampleCleanContext, Name of Sample to Query, 
	  *Attr to Query, Agg Function to Use, Predicate, Sampling Ratio)
	  */
	  def rawSCQuery(scc:SampleCleanContext, sampleName: String, 
	  				  attr: String, expr: String, 
	  				  pred:String, 
	  				  sampleRatio: Double): (Double, Double)=
	  {
	  	  val hc:HiveContext = scc.getHiveContext()
	  	  val hiveTableName = getCleanSampleName(sampleName)

	  	  if (expr.toLowerCase() == "avg"){
	  	  	 
	  	  	 val buildQuery = buildSelectQuery(List(attr,"dup"),
	  	  	 	                               hiveTableName,
	  	  	 	                               pred)
	  	  	 
	  	  	 return approxAvg(hc.hql(buildQuery),sampleRatio)
	  	  }
	  	  else if (expr.toLowerCase() == "sum"){

	  	  	 val buildQuery = buildSelectQuery(
	  	  	 	                 List(predicateToCaseMult(pred,attr)
	  	  	 	                 ,"dup"),
	  	  	 	              hiveTableName)

	  	  	 return approxSum(hc.hql(buildQuery),sampleRatio)
	  	  	}
	  	  else
	  	  {
	  	  	 val buildQuery = buildSelectQuery(
	  	  	 	                 List(predicateToCase(pred)
	  	  	 	                 ,"dup"),
	  	  	 	              hiveTableName)

	  	  	 return approxCount(hc.hql(buildQuery),sampleRatio)
	  	  }

	  }

	 /*This query executes rawSC with a group given an attribute to aggregate, 
	  * expr {SUM, COUNT, AVG}, a predicate, and the sampling ratio.
	  * It returns a tuple of the estimate, and the variance of the estimate (EST, VAR_EST)

	  *Args (SampleCleanContext, Name of Sample to Query, 
	  *Attr to Query, Agg Function to Use, Predicate, Sampling Ratio)
	  */
	 def rawSCQueryGroup(scc:SampleCleanContext, sampleName: String, 
	  				  attr: String, expr: String, 
	  				  pred:String,
	  				  group: String, 
	  				  sampleRatio: Double): (Long, List[(String, (Double, Double))])=
	  {
	  	  	val hc:HiveContext = scc.getHiveContext()
	  	  	val hiveTableName = getDirtySampleName(sampleName)

	  	  	val distinctKeys = hc.hql(buildSelectDistinctQuery(List(group),
	  	  													   hiveTableName, 
	  	  													   "true"))
	  	  						 .map(x => x.getString(0)).collect()

	  		var result = List[(String, (Double, Double))]()
	  		for(t <- distinctKeys)
	  			{ 
	  				result = (t, rawSCQuery(scc, sampleName, 
	  							   attr, expr, 
	  							   appendToPredicate(pred, attrEquals(group,t)),
	  							   sampleRatio)) :: result
	  			}

	  		return (System.nanoTime, result)
	  }

	  /*This query executes rawSC given an attribute to aggregate, expr {SUM, COUNT, AVG}, a predicate, and the sampling ratio.
	  * It returns a tuple of the estimate, and the variance of the estimate (EST, VAR_EST)
	  *
	  *Args (SampleCleanContext, Name of Sample to Query, 
	  *Attr to Query, Agg Function to Use, Predicate, Sampling Ratio)
	  */
	 def normalizedSCQuery(scc:SampleCleanContext, sampleName: String, 
	  				  attr: String, expr: String, 
	  				  pred:String, 
	  				  sampleRatio: Double): (Double, Double)=
	  {
	  	  val hc:HiveContext = scc.getHiveContext()
	  	  val baseTableClean = getCleanSampleName(sampleName)
	  	  val baseTableDirty = getDirtySampleName(sampleName)

	  	  val newPred = makeExpressionExplicit(pred,baseTableClean)
	  	  val oldPred = makeExpressionExplicit(pred,baseTableDirty)
	  	  val typeSafeCleanAttr = makeExpressionExplicit(typeSafeHQL(attr),baseTableClean)
	  	  val typeSafeDirtyAttr = makeExpressionExplicit(typeSafeHQL(attr),baseTableDirty)
	  	  val typeSafeDup = makeExpressionExplicit(typeSafeHQL("dup",1),baseTableClean)

	  	  val selectionStringAVG = subtract(typeSafeDirtyAttr, divide(typeSafeCleanAttr,typeSafeDup) )

	  	  val selectionStringSUM = subtract( parenthesize( predicateToCaseMult(typeSafeDirtyAttr,oldPred)),
	  	  	                            divide(parenthesize(predicateToCaseMult(typeSafeCleanAttr,newPred)),typeSafeDup))

	  	  val selectionStringCOUNT = subtract( parenthesize( predicateToCase(oldPred)),
	  	  	                            divide(parenthesize(predicateToCase(newPred)),typeSafeDup))

	  	  var query = ""
	  	  if (expr.toLowerCase() == "avg"){
			val buildQuery = buildSelectQuery(List(selectionStringAVG,"1"),
				                           baseTableClean,
				                           pred,
				                           baseTableDirty,
				                           "hash")
	  	  	 return approxAvg(hc.hql(buildQuery),sampleRatio)
	  	  }
	  	  else if (expr.toLowerCase() == "sum"){
			val buildQuery = buildSelectQuery(List(selectionStringSUM,"1"),
				                           baseTableClean,
				                           "true",
				                           baseTableDirty,
				                           "hash")
	  	  	 return approxSum(hc.hql(buildQuery),sampleRatio)
	  	  	}
	  	  else
	  	  {
			val buildQuery = buildSelectQuery(List(selectionStringCOUNT,"1"),
				                           baseTableClean,
				                           "true",
				                           baseTableDirty,
				                           "hash")
	  	  	 return approxSum(hc.hql(buildQuery),sampleRatio)
	  	  }

	  }

	  	 /*This query executes normalizedSC with a group given an attribute to aggregate, 
	  * expr {SUM, COUNT, AVG}, a predicate, and the sampling ratio.
	  * It returns a tuple of the estimate, and the variance of the estimate (EST, VAR_EST)

	  *Args (SampleCleanContext, Name of Sample to Query, 
	  *Attr to Query, Agg Function to Use, Predicate, Sampling Ratio)
	  */
	 def normalizedSCQueryGroup(scc:SampleCleanContext, sampleName: String, 
	  				  attr: String, expr: String, 
	  				  pred:String,
	  				  group: String, 
	  				  sampleRatio: Double): List[(String, (Double, Double))]=
	  {
	  	  	val hc:HiveContext = scc.getHiveContext()
	  	  	val hiveTableName = getDirtySampleName(sampleName)

	  	  	val distinctKeys = hc.hql(buildSelectDistinctQuery(List(group),
	  	  													   hiveTableName, 
	  	  													   "true"))
	  	  						 .map(x => x.getString(0)).collect()

	  		var result = List[(String, (Double, Double))]()
	  		for(t <- distinctKeys)
	  			{
	  				result = (t, normalizedSCQuery(scc, sampleName, 
	  							   attr, expr, 
	  							   appendToPredicate(pred, attrEquals(group,t)),
	  							   sampleRatio)) :: result
	  			}

	  		return result
	  }


	///fix
	def compareQueryResults(qr1:(Long, List[(String, (Double, Double))]),
							qr2:(Long, List[(String, (Double, Double))])): (Long, List[(String, Double)]) = {

		val timeStamp = Math.max(qr1._1,qr2._1)
		var result = List[(String, Double)]()
		for(k1 <- qr1._2)
		{
			for(k2 <- qr2._2)
			{
				
				if(k1._1.equals(k2._1))
				{
					val diff = k2._2._1 - k1._2._1
					//val std = 2*k2._2._2

					if(Math.abs(diff) > 0.0)
						result = (k1._1, k2._2._1 - k1._2._1) :: result
				}
			}
		}

		return (timeStamp, result)
	} 
}
