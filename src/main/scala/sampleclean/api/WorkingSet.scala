package sampleclean.api

import org.apache.spark.sql.SchemaRDD
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import scala.util.Random
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

class WorkingSet(scc:SampleCleanContext, tableName:String) {

	  def query(sql:String):SchemaRDD = {
	  	  if (sql.toLowerCase().contains("selectnsc") ||
	  	  	  sql.toLowerCase().contains("selectrawsc"))
	  	  	return scc.hql(sql.replace("$t"," " + tableName +" "))
	  	  else
	  	  	return scc.hql(sql.replace("$t"," " + scc.qb.getCleanSampleName(tableName) +" "))

	  }

	  def view(sql:String):WorkingSet = {
	  	  if (sql.toLowerCase().contains("selectnsc") ||
	  	  	  sql.toLowerCase().contains("selectrawsc"))
	  	  	{
	  	  		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
	  	  		scc.hql(sql.replace("$t"," " + tableName +" ")).saveAsTable(tmpTableName)
	  	  		scc.initialize(tmpTableName,tmpTableName+"_sample")
	  	  		return new WorkingSet(scc,tmpTableName+"_sample")
	  	  	}
	  	  else
	  	  	{ 
	  	  		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
	  	  		scc.hql(sql.replace("$t"," " + scc.qb.getCleanSampleName(tableName) +" ")).saveAsTable(tmpTableName)
	  	  		scc.initialize(tmpTableName,tmpTableName+"_sample")
	  	  		return new WorkingSet(scc,tmpTableName+"_sample")
	  	  	}

	  }

	  def oneHotEncoding(col:RDD[String]):RDD[List[Double]] = {

	  	  val dict:Array[String] = col.flatMap(_.split(" ")).distinct().collect()
	  	  val dictMap = dict.zipWithIndex.toMap
	  	  val broadcastVar = scc.getSparkContext().broadcast(dictMap)
	  	  val result = col.map( x => 
	  	  						Vectors.sparse(dict.length, 
	  	  									   x.split(" ")
	  	  									   		.toSeq.map(y => 
	  	  									   			(broadcastVar.value(y),1.0)
	  	  									   )).toArray.toList
	  	  					  )

	  	  return result

	  }

	  def numericalFeature(col:RDD[String], parseError:Double=0.0):RDD[List[Double]] = {
	  	  return col.map(x => Vectors.sparse(1,List((0,try { x.toDouble } catch { case _ => parseError}))).toArray.toList)
	  }

	  def featureView(features:List[(String,String)]):RDD[Vector] = {

	  	  var featureList:List[RDD[List[Double]]] = List[RDD[List[Double]]]()
	  	  for(feature <- features)
	  	  {
	  	  		val data = scc.hql("select " + feature._1 + " from " + scc.qb.getCleanSampleName(tableName))
	  	  		
	  	  		if(feature._2 == "categorical" || feature._2 == "text")
	  	  			featureList = oneHotEncoding(data.rdd.map(x => x(0).asInstanceOf[String])) :: featureList
	  	  		else
	  	  			featureList = numericalFeature(data.rdd.map(x => x(0).asInstanceOf[String])) :: featureList
	  	  }

	  	  // zip the RDDs into an RDD of Seq[Int]
			def makeZip(s: Seq[RDD[List[Double]]]): RDD[Seq[List[Double]]] = {
  			if (s.length == 1) 
    			s.head.map(e => Seq(e)) 
  			else {
    			val others = makeZip(s.tail)
    			val all = s.head.zip(others)
    			all.map(elem => Seq(elem._1) ++ elem._2)
  				}
			}

			// zip and apply arbitrary function from Seq[Int] to Int
			def applyFuncToZip(s: Seq[RDD[List[Double]]], 
							   f:Seq[List[Double]] 
							   		=> List[Double]): RDD[List[Double]] = {
  				val z = makeZip(s)
  				z.map(f)
			}

			val res = applyFuncToZip(featureList, 
									  {s : Seq[List[Double]] 
									 		=> s.foldLeft(List[Double]())
									 				{(a:List[Double],b:List[Double]) => a ::: b}
									  }
									)

	  	  return res.map(x => Vectors.dense(x.toArray))
	  }

	  def clean(contextFreeAlgorithm: (SampleCleanContext,String) 
	  						=> SampleCleanAlgorithm):WorkingSet =
	  {
	  	val timeStart = System.currentTimeMillis
	  	val algorithm = contextFreeAlgorithm(scc, tableName)
	  	algorithm.exec()
	  	println("Exec Time: " + (System.currentTimeMillis - timeStart))
	  	return this
	  }

}