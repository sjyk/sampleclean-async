package sampleclean.crowd.context

/**
 * Parameters for labeling a entity resolution task
 * @param content a list of two lists, which contains the corresponding values of each record
 */
case class DeduplicationPointLabelingContext(content: List[List[Any]]) extends PointLabelingContext


/** Group Context subclass for deduplication
  * @param taskType the type of all the points in the group
  * @param data the group context that is shared among all the points
  * */
case class DeduplicationGroupLabelingContext(taskType: String="er", data: Map[String, Any])
  extends GroupLabelingContext