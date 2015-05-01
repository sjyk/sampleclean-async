package sampleclean.activeml

import org.apache.spark.mllib.tree.model.{Node}
import org.apache.spark.mllib.linalg.Vector

class TreePath(val rootNode: Node, val leafNode: Node) {
  assert(leafNode.isLeaf)
  val prediction = leafNode.predict

  def matchesPath(features: Vector, curNode:Node = rootNode): Boolean = {
    if (curNode == leafNode) true else {
      if (features(curNode.split.get.feature) <= curNode.split.get.threshold) {
        matchesPath(features, curNode.leftNode.get)
      } else {
        matchesPath(features, curNode.rightNode.get)
      }
    }
  }
}
