package sampleclean.api

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import sampleclean.util.TypeUtils._

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
	  	  val hiveTableName = scc.qb.getCleanSampleName(sampleName)
	  	  
	  	  var defaultPred = "true"
	  	  val dup = scc.qb.exprToDupString(sampleName)
	  	  val tattr = scc.qb.transformExplicitExpr(attr,sampleName)

	  	  if(pred != "")
	  	  	defaultPred = scc.qb.transformExplicitExpr(pred,sampleName)

	  	  if (expr.toLowerCase() == "avg"){
	  	  	 
	  	  	 val buildQuery = scc.qb.buildSelectQuery(List(tattr,dup),
	  	  	 	                               hiveTableName,
	  	  	 	                               defaultPred)
	  	  	 
	  	  	 return approxAvg(hc.hql(buildQuery),sampleRatio)
	  	  }
	  	  else if (expr.toLowerCase() == "sum"){

	  	  	 val buildQuery = scc.qb.buildSelectQuery(
	  	  	 	                 List(scc.qb.predicateToCaseMult(defaultPred,tattr)
	  	  	 	                 ,dup),
	  	  	 	              hiveTableName)

	  	  	 return approxSum(hc.hql(buildQuery),sampleRatio)
	  	  	}
	  	  else
	  	  {
	  	  	 val buildQuery = scc.qb.buildSelectQuery(
	  	  	 	                 List(scc.qb.predicateToCase(defaultPred)
	  	  	 	                 ,dup),
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
	  	  	val hiveTableName = scc.qb.getDirtySampleName(sampleName)

	  	  	val distinctKeys = hc.hql(scc.qb.buildSelectDistinctQuery(List(scc.qb.transformExplicitExpr(group,sampleName,true)),
	  	  													   hiveTableName, 
	  	  													   "true"))
	  	  						 .map(x => x(0).asInstanceOf[String]).collect()

	  		var result = List[(String, (Double, Double))]()
	  		for(t <- distinctKeys)
	  			{ 
	  				if(pred != ""){
	  					result = (t, rawSCQuery(scc, sampleName, 
	  							   attr, expr, 
	  							   scc.qb.appendToPredicate(pred, 
	  							   				scc.qb.attrEquals(group,t)),
	  							   sampleRatio)) :: result
	  				}
	  				else{
	  					result = (t, rawSCQuery(scc, sampleName, 
	  							   attr, expr,
	  							   scc.qb.attrEquals(group,t),
	  							   sampleRatio)) :: result
						}
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
	  	  val baseTableClean = scc.qb.getCleanSampleName(sampleName)
	  	  val baseTableDirty = scc.qb.getDirtySampleName(sampleName)

	  	  var defaultPred = "true"
	  	  if(pred != "")
	  	  	defaultPred = pred

	  	  val newPred = scc.qb.makeExpressionExplicit(defaultPred,baseTableClean)
	  	  val oldPred = scc.qb.makeExpressionExplicit(defaultPred,baseTableDirty)
	  	  val typeSafeCleanAttr = scc.qb.makeExpressionExplicit(typeSafeHQL(attr),baseTableClean)
	  	  val typeSafeDirtyAttr = scc.qb.makeExpressionExplicit(typeSafeHQL(attr),baseTableDirty)
	  	  val typeSafeDup = scc.qb.makeExpressionExplicit(typeSafeHQL("dup",1),baseTableClean)

	  	  val selectionStringAVG = scc.qb.subtract(typeSafeDirtyAttr, scc.qb.divide(typeSafeCleanAttr,typeSafeDup) )

	  	  val selectionStringSUM = scc.qb.subtract( scc.qb.parenthesize( scc.qb.predicateToCaseMult(typeSafeDirtyAttr,oldPred)),
	  	  	                            scc.qb.divide(scc.qb.parenthesize(scc.qb.predicateToCaseMult(typeSafeCleanAttr,newPred)),typeSafeDup))

	  	  val selectionStringCOUNT = scc.qb.subtract( scc.qb.parenthesize( scc.qb.predicateToCase(oldPred)),
	  	  	                            scc.qb.divide(scc.qb.parenthesize(scc.qb.predicateToCase(newPred)),typeSafeDup))

	  	  var query = ""
	  	  if (expr.toLowerCase() == "avg"){
			val buildQuery = scc.qb.buildSelectQuery(List(selectionStringAVG,"1"),
				                           baseTableClean,
				                           pred,
				                           baseTableDirty,
				                           "hash")
	  	  	 return approxAvg(hc.hql(buildQuery).cache(),sampleRatio)
	  	  }
	  	  else if (expr.toLowerCase() == "sum"){
			val buildQuery = scc.qb.buildSelectQuery(List(selectionStringSUM,"1"),
				                           baseTableClean,
				                           "true",
				                           baseTableDirty,
				                           "hash")
	  	  	 return approxSum(hc.hql(buildQuery).cache(),sampleRatio)
	  	  	}
	  	  else
	  	  {
			val buildQuery = scc.qb.buildSelectQuery(List(selectionStringCOUNT,"1"),
				                           baseTableClean,
				                           "true",
				                           baseTableDirty,
				                           "hash")

			println(buildQuery)
	  	  	 return approxSum(hc.hql(buildQuery).cache(),sampleRatio)
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
	  				  sampleRatio: Double): (Long,List[(String, (Double, Double))])=
	  {
	  	  	val hc:HiveContext = scc.getHiveContext()
	  	  	val hiveTableName = scc.qb.getDirtySampleName(sampleName)

	  	  	val distinctKeys = hc.hql(scc.qb.buildSelectDistinctQuery(List(group),
	  	  													   hiveTableName, 
	  	  													   "true"))
	  	  						 .map(x => x.getString(0)).collect()

	  		var result = List[(String, (Double, Double))]()
	  		for(t <- distinctKeys)
	  			{
	  				result = (t, normalizedSCQuery(scc, sampleName, 
	  							   attr, expr, 
	  							   scc.qb.appendToPredicate(pred, 
	  							   		scc.qb.attrEquals(group,t)),
	  							   sampleRatio)) :: result
	  			}

	  		return (System.nanoTime, result)
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
