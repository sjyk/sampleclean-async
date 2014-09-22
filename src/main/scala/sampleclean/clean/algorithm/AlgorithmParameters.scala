package sampleclean.clean.algorithm

@serializable
class AlgorithmParameters() {

	var params = List[Any]()
	var names = List[String]()

	def put(name:String,param:Any)={
		names = name :: names
		params = param :: params
	}

	def get(name:String):Any = {
		return params(names.indexOf(name))
	}

  def exist(name:String): Boolean = {
    return names.exists(_ == name)
  }

}