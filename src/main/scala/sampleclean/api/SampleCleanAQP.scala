package sampleclean.api

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext
import scala.util.Random

import sampleclean.util.TypeUtils._


/**
 * This class provides the approximate query processing
 * for SampleClean. Currently, it supports SUM, COUNT, AVG
 * and returns confidence intervals in the form of CLT variance
 * estimates.
 */
@serializable
class SampleCleanAQP() {

	  /**
     * Internal method to calculate the normalization for the AVG query
	   */
	  private def duplicationRate(rdd:SchemaRDD):Double=
	  {
	  	  return rdd.count()/rdd.map( x => 1.0/x(1).asInstanceOf[Int]).reduce(_ + _)
	  }

  // TODO: group? preserveTableName?
  /**
   * This query executes rawSC with a group given an attribute to aggregate,
   * expr {SUM, COUNT, AVG}, a predicate, and the sampling ratio.
   * It returns a tuple of the estimate, and the variance of the estimate (EST, VAR_EST)
   * @param attr attribute to query
   * @param expr aggregate function to use
   * @param pred predicate
   * @param group
   * @param sampleRatio sampling ratio
   * @param preserveTableName
   */
	 def rawSCQueryGroup(scc:SampleCleanContext, sampleName: String, 
	  				  attr: String, expr: String, 
	  				  pred:String,
	  				  group: String, 
	  				  sampleRatio: Double,
	  				  preserveTableName:Boolean =false): (Long, List[(String, (Double, Double))])=
	  {
	  	  	val hc:HiveContext = scc.getHiveContext()

	  	  	var hiveTableName = scc.qb.getCleanSampleName(sampleName)

	  	  	if(preserveTableName)
	  	  		hiveTableName = sampleName

	  	  	var defaultPred = "true"
	  	  	val dup = scc.qb.exprToDupString(sampleName)
	  	  	val tattr = scc.qb.transformExplicitExpr(attr,sampleName)
	  	  	var gattr = scc.qb.transformExplicitExpr(group,sampleName)
	  	  	
	  	  	if(group == "")
	  	  		gattr = "'1'"

	  	  	val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))

	  	  	val k = hc.hql("SELECT 1 from " + hiveTableName).count()

	  	  	if(pred != "")
	  	  		defaultPred = scc.qb.transformExplicitExpr(pred,sampleName)

	  		if (expr.toLowerCase() == "avg"){
	  	  	 
	  	  	 val buildQuery = scc.qb.buildSelectQuery(
	  	  	 	                 List(scc.qb.divide(scc.qb.predicateToCaseMult(defaultPred,tattr), dup) + " as agg",
	  	  	 	                 gattr + " as group"),
	  	  	 	              hiveTableName)

	  	  	 println(buildQuery)

	  	  	 hc.registerRDDAsTable(hc.hql(buildQuery),tmpTableName)

	  	  	 val aggQuery = scc.qb.buildSelectQuery(List( "group",
	  	  	 										      "avg(agg)",
	  	  	 	                 					      "var_samp(agg)/"+k),
	  	  	 	              					    tmpTableName) +
	  	  	 				" group by group"

	  	  	 println(aggQuery)
	  	  	 val result = hc.hql(aggQuery).map( row => (row(0).asInstanceOf[String],
	  	  	 					      (row(1).asInstanceOf[Double],
	  	  	 					      Math.sqrt(row(2).asInstanceOf[Double])))).collect()

	  	  	 return (System.nanoTime, result.toList)
	  	  	}
	  	  	else if (expr.toLowerCase() == "sum"){

	  	  	 val buildQuery = scc.qb.buildSelectQuery(
	  	  	 	                 List(scc.qb.divide(scc.qb.predicateToCaseMult(defaultPred,tattr), dup) + " as agg",
	  	  	 	                 gattr + " as group"),
	  	  	 	              hiveTableName)

	  	  	 hc.registerRDDAsTable(hc.hql(buildQuery),tmpTableName)

	  	  	 val aggQuery = scc.qb.buildSelectQuery(List( "group",
	  	  	 										      "sum(agg)/"+sampleRatio,
	  	  	 	                 					      scc.qb.countSumVarianceSQL(k,"agg",sampleRatio)),
	  	  	 	              					    tmpTableName) +
	  	  	 				" group by group"

	  	  	 println(aggQuery)
	  	  	 val result = hc.hql(aggQuery).map( row => (row(0).asInstanceOf[String],
	  	  	 					      (row(1).asInstanceOf[Double],
	  	  	 					      Math.sqrt(row(2).asInstanceOf[Double])))).collect()
	  	  	 return (System.nanoTime, result.toList)
	  	  	}
	  	  	else
	  	  	{
	  	  	 val buildQuery = scc.qb.buildSelectQuery(
	  	  	 	                 List(scc.qb.divide(scc.qb.predicateToCase(defaultPred), dup) + " as agg",
	  	  	 	                 gattr + " as group"),
	  	  	 	              hiveTableName)

	  	  	 hc.registerRDDAsTable(hc.hql(buildQuery),tmpTableName)

	  	  	 val aggQuery = scc.qb.buildSelectQuery(List( "group",
	  	  	 										      "sum(agg)/"+sampleRatio,
	  	  	 	                 					      scc.qb.countSumVarianceSQL(k,"agg",sampleRatio)),
	  	  	 	              					    tmpTableName) +
	  	  	 				" group by group"

	  	  	 println(aggQuery)
	  	  	 val result = hc.hql(aggQuery).map( row => (row(0).asInstanceOf[String],
	  	  	 					      (row(1).asInstanceOf[Double],
	  	  	 					      Math.sqrt(row(2).asInstanceOf[Double])))).collect()
	  	  	 return (System.nanoTime, result.toList)
	  	  	}

	  		//return (System.nanoTime, List(("test",(0.0,0.0))))
	  }

	 def materializeJoinResult(scc:SampleCleanContext, 
	 						    sampleName1: String,
	 						    sampleName2: String, 
	 						    key1:String, 
	 						    key2:String, 
	 						    tableName:String):(SchemaRDD,SchemaRDD) = {

	 	val hc:HiveContext = scc.getHiveContext()
	 	val cleanName1 = scc.qb.getCleanSampleName(sampleName1)
	 	val cleanName2 = scc.qb.getCleanSampleName(sampleName2)
	 	val dirtyName1 = scc.qb.getDirtySampleName(sampleName1)
	 	val dirtyName2 = scc.qb.getDirtySampleName(sampleName2)

	 	val query = scc.qb.createTableAs(scc.qb.getCleanSampleName(tableName))+
	 						scc.qb.buildSelectQuery(scc.qb.getTableJoinSchemaList(cleanName1,cleanName2),
	 												cleanName1,"true",cleanName2,key1,key2,false)

	 	val query1 = hc.hql(scc.qb.createTableAs(scc.qb.getCleanSampleName(tableName))+
	 						scc.qb.buildSelectQuery(scc.qb.getTableJoinSchemaList(cleanName1,cleanName2),
	 												cleanName1,"true",cleanName2,key1,key2,false))

	 	val query2 = hc.hql(scc.qb.createTableAs(scc.qb.getDirtySampleName(tableName))+
	 						scc.qb.buildSelectQuery(scc.qb.getTableJoinSchemaList(dirtyName1,dirtyName2),
	 											    dirtyName1,"true",dirtyName2,key1,key2,true))

	 	return (query1, query2)
	 }

	 def materializeJoinResult(scc:SampleCleanContext, expr:String,tableName:String):(SchemaRDD,SchemaRDD)= {
	 	val params = scc.qb.joinExpr(expr)
	 	return materializeJoinResult(scc,params._1,params._2,params._3,params._4,tableName)
	 }

	  /*Args (SampleCleanContext, Name of Sample to Query, 
	  *Attr to Query, Agg Function to Use, Predicate, Sampling Ratio)
	  */
	 def normalizedSCQueryGroup(scc:SampleCleanContext, sampleName: String, 
	  				  attr: String, expr: String, 
	  				  pred:String,
	  				  group: String, 
	  				  sampleRatio: Double): (Long,List[(String, (Double, Double))])=
	  {
	  	  val hc:HiveContext = scc.getHiveContext()
	  	  var baseTableClean = ""
	  	  var baseTableDirty = ""

	  	  if(scc.qb.isJoinQuery(sampleName))
	  	  {
	  	  		val tmpJTableName = "tmp"+Math.abs((new Random().nextLong()))
	  	  		materializeJoinResult(scc,sampleName,tmpJTableName)
	  	  		baseTableClean = scc.qb.getCleanFactSampleName(tmpJTableName,false)
	  	  		baseTableDirty = scc.qb.getCleanFactSampleName(tmpJTableName,true)
	  	  }
	  	  else{
	  	  		baseTableClean = scc.qb.getCleanFactSampleName(sampleName,false)
	  	  		baseTableDirty = scc.qb.getCleanFactSampleName(sampleName,true)
	  	  }


	  	  var defaultPred = "true"
	  	  if(pred != "")
	  	  	defaultPred = pred

	  	  var gattr = scc.qb.makeExpressionExplicit(group, scc.qb.getCleanSampleName(sampleName))

	  	  val k = hc.hql("SELECT 1 from " + scc.qb.getCleanFactSampleName(sampleName,true)).count() + 0.0
	  	  	
	  	  if(group == "")
	  	  	 gattr = "'1'"

	  	  val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))

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
			val buildQuery = scc.qb.buildSelectQuery(List(selectionStringAVG + " as agg", gattr + " as group"),
				                           baseTableClean,
				                           pred,
				                           baseTableDirty,
				                           "hash")

	  	  	 hc.registerRDDAsTable(hc.hql(buildQuery),tmpTableName)

	  	  	 val aggQuery = scc.qb.buildSelectQuery(List( "group",
	  	  	 										      "avg(agg)",
	  	  	 	                 					      "var_samp(agg)/"+k),
	  	  	 	              					    tmpTableName) +
	  	  	 				" group by group"

	  	  	 println(aggQuery)
	  	  	 val result = hc.hql(aggQuery).map( row => (row(0).asInstanceOf[String],
	  	  	 					      (row(1).asInstanceOf[Double],
	  	  	 					      Math.sqrt(row(2).asInstanceOf[Double])))).collect()
	  	  	 return (System.nanoTime, result.toList)
	  	  }
	  	  else if (expr.toLowerCase() == "sum"){
			val buildQuery = scc.qb.buildSelectQuery(List(selectionStringSUM+ " as agg", gattr + " as group"),
				                           baseTableClean,
				                           "true",
				                           baseTableDirty,
				                           "hash")

	  	  	 hc.registerRDDAsTable(hc.hql(buildQuery),tmpTableName)

	  	  	 val aggQuery = scc.qb.buildSelectQuery(List( "group",
	  	  	 										      "sum(agg)/"+sampleRatio,
	  	  	 	                 					      scc.qb.countSumVarianceSQL(k,"agg",sampleRatio)),
	  	  	 	              					    tmpTableName) +
	  	  	 				" group by group"

	  	  	 println(aggQuery)
	  	  	 val result = hc.hql(aggQuery).map( row => (row(0).asInstanceOf[String],
	  	  	 					      (row(1).asInstanceOf[Double],
	  	  	 					      Math.sqrt(row(2).asInstanceOf[Double])))).collect()
	  	  	 return (System.nanoTime, result.toList)
	  	  	}
	  	  else
	  	  {
	  	  	val cleanCount = rawSCQueryGroup(scc, baseTableClean, attr, expr, pred, group, sampleRatio, true)
	  	  	val dirtyCount = rawSCQueryGroup(scc, baseTableDirty, attr, expr, pred, group, sampleRatio, true)
	  	  	val comparedResult = compareQueryResults(dirtyCount,cleanCount)

			/*val buildQuery = scc.qb.buildSelectQuery(List(selectionStringCOUNT+ " as agg", gattr + " as group"),
				                           baseTableClean,
				                           "true",
				                           baseTableDirty,
				                           "hash")
			println(buildQuery)
			hc.registerRDDAsTable(hc.hql(buildQuery),tmpTableName)

	  	  	 val aggQuery = scc.qb.buildSelectQuery(List( "group",
	  	  	 										      "sum(agg)/"+sampleRatio,
	  	  	 	                 					      scc.qb.countSumVarianceSQL(k,"agg",sampleRatio)),
	  	  	 	              					    tmpTableName) +
	  	  	 				" group by group"

	  	  	 println(aggQuery)
	  	  	 val result = hc.hql(aggQuery).map(row => (row(0).asInstanceOf[String],
	  	  	 					      (row(1).asInstanceOf[Double],
	  	  	 					      row(2).asInstanceOf[Double]))).collect()*/
	  	  	 return (System.nanoTime, comparedResult._2.map(x => (x._1,(x._2,deltaCountToVariance(x._2,k,sampleRatio)))))
	  	  }

	  		return (System.nanoTime, List(("1",(0.0,0.0))))
	  }

	private [sampleclean] def deltaCountToVariance(c:Double,k:Double,sampleRatio:Double):Double={
		val n = k/sampleRatio
		return n*Math.sqrt((1-c/n)*c/n)/Math.sqrt(k)
	}


	private [sampleclean] def compareQueryResults(qr1:(Long, List[(String, (Double, Double))]),
							qr2:(Long, List[(String, (Double, Double))])): (Long, List[(String, Double)]) = {

		val timeStamp = Math.max(qr1._1,qr2._1)
		val hashJoinSet = qr2._2.filter(x => x._1 != null).map(x => (x._1.trim.toLowerCase,x._2)).toMap
		var result = List[(String, Double)]()
		for(k1 <- qr1._2)
		{
			if(k1._1 != null && hashJoinSet.contains(k1._1.trim.toLowerCase))
			{
				val diff = k1._2._1 - hashJoinSet(k1._1.trim.toLowerCase)._1
				//println(k1._1 + " " + k1._2._1 + " " + hashJoinSet(k1._1.trim.toLowerCase)._1)
				result = (k1._1, -diff) :: result
			}
		}
		println("Finished")
		return (timeStamp, result)
	} 
}
