package sampleclean.eval

import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

class Evaluator(scc:SampleCleanContext,
				algorithms:List[SampleCleanAlgorithm], 
				unaryConstraints:scala.collection.mutable.Map[(String,String),String] 
									=  scala.collection.mutable.Map(),
				binaryConstraints:scala.collection.mutable.Map[(String,String,String), Boolean] 
									=  scala.collection.mutable.Map())
				extends Serializable
{

	  var binaryKeySet:Set[String] = Set()
	  var resultTable:List[(Double, Double, Double)] = List()

	  def addUnaryConstraint(hash:String, attr:String, value:String) ={
	  	  unaryConstraints((hash,attr)) = value
	  }

	  def addBinaryConstraint(hash1:String, hash2:String, attr:String, same:Boolean) ={
	  	  binaryConstraints((hash1,hash2,attr))= same
	  	  binaryKeySet += hash1
	  	  binaryKeySet += hash2
	  }

	  def setAlgorithmsToSample(sampleName:String) = {
	  	for(a <- algorithms)
	  		a.setSampleName(sampleName)
	  }

	  def compareValues(row:Row, 
	  					schemaLength:Int , 
	  					schemaMap:Map[Int,String], 
	  					gt:scala.collection.mutable.Map[(String,String),String]):(Int, Int) = {

	  		var hit = 0
	  		var total = 0
	  		for(i <- 1 until schemaLength)
	  		{
	  			val attr = schemaMap(i)
	  			val hash = row(0).asInstanceOf[String]
	  			
	  			if(gt contains (hash,attr))
	  			{	total = total + 1
	  				if(gt((hash,attr)) == row(i).toString())
	  					hit = hit + 1
	  			}
	  		}

	  		return (total,hit)
	  }	

	  def compairPairs(row1:Row,
	  				   row2:Row,
	  				   schemaLength:Int , 
	  				   schemaMap:Map[Int,String],
	  				   gt:scala.collection.mutable.Map[(String,String,String), Boolean]):(Int,Int) ={

	  		var hit = 0
	  		var total = 0
	  		for(i <- 1 until schemaLength)
	  		{
	  			val attr = schemaMap(i)
	  			val hash1 = row1(0).asInstanceOf[String]
	  			val hash2 = row2(0).asInstanceOf[String]
	  			
	  			if(gt contains (hash1,hash2,attr))
	  			{	total = total + 1
	  				if(gt((hash1,hash2,attr)))
	  				{
	  					if(row1(i) == row2(i))
	  						hit = hit + 1
	  				}
	  				else{

	  					if(row1(i) != row2(i))
	  						hit = hit + 1

	  				}
	  			}
	  		}

	  		return (total,hit)
	  }

	  def evaluate(sampleName:String) = {
	  	  println("This procedure will remove any changes in your sample. Rebase to save changes. ")
	  	  val schema = scc.getHiveTableSchema(scc.qb.getCleanSampleName(sampleName))
	  	  var schemaMap:Map[Int,String] = Map()
	  	  for (i <- 0 until schema.length)
	  	  	schemaMap = schemaMap + (i -> schema(i)) 

	  	  for(a <- algorithms)
	  	  {
	  	  	  scc.resetSample(sampleName)
	  	  	  val timeStart = System.currentTimeMillis;
	  	  	  val result = a.synchronousExecAndRead()
	  	  	  val totalTime = (System.currentTimeMillis-timeStart)/1000.0
	  	  	  val accTuples:RDD[(Int,Int)] = result.map(compareValues(_,schema.length, schemaMap, unaryConstraints))
	  	  	  val unaryResults = accTuples.reduce((x:(Int,Int),y:(Int,Int)) => (x._1+y._1,x._2+y._2))
			  val binarySet = result.filter(x => binaryKeySet.contains(x(0).asInstanceOf[String]))
			  val pairs = binarySet.cartesian(binarySet)
			  val accBTuples:RDD[(Int,Int)] = pairs.map(x => compairPairs(x._1,x._2,schema.length, schemaMap, binaryConstraints))
			  val binaryResults = accBTuples.reduce((x:(Int,Int),y:(Int,Int)) => (x._1+y._1,x._2+y._2))
			  resultTable = ((unaryResults._2+0.0)/(unaryResults._1), 
			  				 (binaryResults._2+0.0)/(binaryResults._1),
			  				 totalTime) :: resultTable 	
	  	  }
	  	  scc.resetSample(sampleName)
	  	  resultTable = resultTable.reverse
	  	  printResults()
	  } 

	  def printResults() = {
	  	 println("Unary Accuracy\tBinary Accuracy\tTotal Time")
	  	 println("")
	  	 for(r <- resultTable)
	  	 	{
	  	 		println(r._1+"\t\t"+r._2+"\t\t"+r._3)
	  	 	}
	  }
}