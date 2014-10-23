package sampleclean.clean.misc

import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.AlgorithmParameters
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import sampleclean.api.SampleCleanContext._

import sampleclean.util.TypeUtils._

/** Example!! 
*/
@serializable
class CustomTransform(params:AlgorithmParameters,scc: SampleCleanContext) 
         extends SampleCleanAlgorithm(params, scc) {

    val customMaps = Map("microsoft" -> 
    								"Microsoft Research",
    					 "university of california" -> 
    					 			"University of California Berkeley",
    					 "university of california berkeley" ->
    					 			"University of California Berkeley",
    					 "university college" ->
    					 			"University College London"
    					)

    def customTransform(input:String): String ={
        
        if(input == null)
            return input

    	var x = pipeSplit(input)
    	x = x.replace("UC ", "University of California ")
    	x = x.replace("Univ.", "University")

    	if(customMaps.contains(x.trim.toLowerCase))
    		return customMaps.get(x.trim.toLowerCase).get
    	else{

    		if((x.trim.indexOf("D") == 0 ||
    			x.trim.indexOf("School") == 0 ||
    			x.trim.indexOf("Instit") == 0 ||
    			x.trim.indexOf("Faculty") == 0)
    			 && x.indexOf("  ") >= 0)
    		{
    			val university = x.substring(x.indexOf("  ")).trim

    			if(customMaps.contains(university.toLowerCase))
    				return customMaps.get(university.toLowerCase).get
    			
    			return university.split("\\s+").mkString(" ")
    		} 
    		else if (x.trim.indexOf("D") == 0 && x.split("\\s+").length <=6){
    			return "Ambiguous"
    		}

    		return x.split("\\s+").mkString(" ")
    	}
    }

    def pipeSplit(x:String):String = {
    	val comps = x.split("\\|")
    	return comps(Math.min(1,comps.length-1)).trim()
    }

	def exec(tableName:String)={
		val attr = params.get("attr").asInstanceOf[String]
		val attrCol = scc.getColAsIndex(tableName,attr)
    	val hashCol = scc.getColAsIndex(tableName,"hash")

		val update_rdd = scc.getCleanSample(tableName).map(x =>(x(hashCol).asInstanceOf[String],
																  customTransform(x(attrCol).asInstanceOf[String])))
		scc.updateTableAttrValue(tableName, attr, update_rdd)

		this.onUpdateNotify()
	}


}