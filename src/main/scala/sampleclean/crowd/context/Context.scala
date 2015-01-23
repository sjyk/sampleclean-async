package sampleclean.crowd.context

/**
 * The abstract class used to represent the different contexts for different crowd tasks
 * There is only one field data of type T, which stores the necessary information for a context
 */
abstract class PointLabelingContext {
  val content : Object
}


/** An abstract class used to represent the group labeling contexts */
abstract class GroupLabelingContext {
  val taskType : String
  val data : Object
}