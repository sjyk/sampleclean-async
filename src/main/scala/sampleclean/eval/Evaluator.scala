package sampleclean.eval

import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

/**
 * The Evaluator class executes a set of SampleCleanAlgorithms
 * on a sample of data, and compares the result to user specified 
 * constraints. For a relation R, primary key i, and an attribute 
 * a, constraints are of the following form:
 *
 * Unary  R(i,a) == constant
 * Binary R(i,a) !=/== R(j,a)
 *
 * The Evaluator will physically update the specified sample and 
 * then reset it to its dirty version after each algorithm. This allows
 * us to time the write phase of the algorithm. It is up to the user
 * to enforce consistent behavior, as SampleCleanAlgorithms are defined
 * with a sample. There is a "setAlgorithmsToSample" helper method to
 * do this.
 *
 * Definitions
 * Unary Accuracy: Let U be the set of unary constraints.
 * We define unary accuracy to be hits / total .
 * Total = the total i \in U that are also in the sample. 
 * Hits = the total number of tuples in the sample for which the constraint
 * is satisfied.
 *
 * Binary Accuracy: Let B be the set of unary constraints.
 * We define unary accuracy to be hits / total .
 * Total = the total i,j \in B that are also in the sample. 
 * Hits = the total number of !!pairs!! tuples in the sample for which the 
 * constraint is satisfied.
 *
 * The schema of a unary constraint is (Hash, Attribute) -> Value
 * The schema of a binary constraint is (Hash1, Hash2, Attribute) -> Boolean 
 * True for a binary constraint means they are == and false means !=
 * 
 * @type {[type]}
 */
class Evaluator(scc:SampleCleanContext,
				algorithms:List[SampleCleanAlgorithm], 
				unaryConstraints:scala.collection.mutable.Map[(String,String),String] 
									=  scala.collection.mutable.Map(),
				binaryConstraints:scala.collection.mutable.Map[(String,String,String), Boolean] 
									=  scala.collection.mutable.Map())
				extends Serializable
{

	  //This is the key set of the binary constraints i.e., all i  s.t 
	  // there exists a constraint between R(i,a) and some other R(j,a)
	  var binaryKeySet:Set[String] = Set()

	  //This is a result table for storing the result of all of the 
	  //algorithm trials. The table is a list of tuples for every
	  //algorithm. The schema is as follows (Unary Accuracy, Binary Accuracy, and Time (s))
	  var resultTable:List[(Double, Double, Double)] = List()

	  /**
	   * This method adds a unary constraint to the the map
	   * @type {[type]}
	   */
	  def addUnaryConstraint(hash:String, attr:String, value:String) ={
	  	  unaryConstraints((hash,attr)) = value
	  }

	  /**
	   * This method adds a binary constraint to the the map
	   * @type {[type]}
	   */
	  def addBinaryConstraint(hash1:String, hash2:String, attr:String, same:Boolean) ={
	  	  binaryConstraints((hash1,hash2,attr))= same
	  	  binaryKeySet += hash1
	  	  binaryKeySet += hash2
	  }

	  /**
	   * This method (if needed) sets all the algorithms to use
	   * the specified sample. 
	   * @type {[type]}
	   */
	  def setAlgorithmsToSample(sampleName:String) = {
	  	for(a <- algorithms)
	  		a.setSampleName(sampleName)
	  }

	  /**
	   * This method calculates the unary accuracy for one row
	   * @type {[type]}
	   */
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

	  /**
	   * This method calculates the binary accuracy for one row
	   * @type {[type]}
	   */
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

	  /**
	   * The evaluate method executes each of the algorithms
	   * in order on the sample. It records the binary and 
	   * unary accuracy and the time of execution.
	   * @type {[type]}
	   */
	  def evaluate(sampleName:String) = {
	  	  println("This procedure will remove any changes in your sample. Rebase to save changes. ")
	  	 
	  	  //This gets the schema of the sample, it is not strictly neccessary but it is 
	  	  //easier for users to provide the constraints in terms of column names rather
	  	  //than column index.
	  	  val schema = scc.getHiveTableSchema(scc.qb.getCleanSampleName(sampleName))
	  	  var schemaMap:Map[Int,String] = Map()
	  	  for (i <- 0 until schema.length)
	  	  	schemaMap = schemaMap + (i -> schema(i)) 


	  	  for(a <- algorithms)
	  	  {
	  	  	  scc.resetSample(sampleName) //reset the sample after cleaning

	  	  	  val timeStart = System.currentTimeMillis;
	  	  	  val result = a.synchronousExecAndRead() //actual execution
	  	  	  val totalTime = (System.currentTimeMillis-timeStart)/1000.0
	  	  	  
	  	  	  //calculate the unary accuracy
	  	  	  val accTuples:RDD[(Int,Int)] = result.map(compareValues(_,schema.length, schemaMap, unaryConstraints))
	  	  	  val unaryResults = accTuples.reduce((x:(Int,Int),y:(Int,Int)) => (x._1+y._1,x._2+y._2))

	  	  	  //calculate the binary accuracy
			  val binarySet = result.filter(x => binaryKeySet.contains(x(0).asInstanceOf[String]))
			  val pairs = binarySet.cartesian(binarySet)
			  val accBTuples:RDD[(Int,Int)] = pairs.map(x => compairPairs(x._1,x._2,schema.length, schemaMap, binaryConstraints))
			  val binaryResults = accBTuples.reduce((x:(Int,Int),y:(Int,Int)) => (x._1+y._1,x._2+y._2))

			  //update the result table.
			  resultTable = ((unaryResults._2+0.0)/(unaryResults._1), 
			  				 (binaryResults._2+0.0)/(binaryResults._1),
			  				 totalTime) :: resultTable 	
	  	  }

	  	  scc.resetSample(sampleName) //reset the sample
	  	  resultTable = resultTable.reverse

	  	  printResults() //prints a formatted table
	  } 

	  /**
	   * This prints a formatted table of the results
	   */
	  def printResults() = {
	  	 println("Unary Accuracy\tBinary Accuracy\tTotal Time")
	  	 println("")
	  	 for(r <- resultTable)
	  	 	{
	  	 		println(r._1+"\t\t"+r._2+"\t\t"+r._3)
	  	 	}
	  }
}