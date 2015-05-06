package sampleclean.clean.deduplication

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private [sampleclean] object GraphXInterface {

  /**
   * Builds a graph from an RDD of vertex data and an RDD of edge data.
   *
   * @param vertices an RDD of pairs of vertexId, vertexData.
   * @param edges an RDD of tuples of (sourceVertexId, destVertexId, edgeData).
   * @tparam VD The type of data at each vertex
   * @tparam ED The type of data at each edge
   * @return a GraphX graph containing the vertices and edges.
   */
  def buildGraph[VD: ClassTag, ED: ClassTag](vertices: RDD[(Long, VD)], edges: RDD[(Long, Long, ED)]): Graph[VD, ED] =
    Graph(vertices, edges.map(pair => Edge(pair._1, pair._2, pair._3)))

  /** Add edges to the graph.
    * No new vertices can be added.
    *
    * @param graph the already extant graph.
    * @param newEdges pairs of (sourceVertexId, destVertexId) referring to existing vertices in graph.
    * @tparam VD The type of data at each vertex
    * @tparam ED The type of data at each edge
    * @return a graph with the new edges added.
    */
  def addEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], newEdges: RDD[(Long, Long, ED)]): Graph[VD, ED] = {
    // TODO: Incrementally update the routing tables
    Graph(graph.vertices, graph.edges.union(newEdges.map(pair => Edge(pair._1, pair._2, pair._3))))
  }

  /**
   * Run connected components on the graph.
   * This implementation merges data within each component, so each vertex in a component will end up with the same data.
   *
   * @param graph a Graphx graph
   * @param reduceFunc A function that merges the data at two vertices in a component.
   * @tparam VD The type of data at each vertex
   * @tparam ED The type of data at each edge
   * @return An RDD of pairs of (VertexId, ComponentData). Every vertex in the same connected component will have the
   *         same ComponentData.
   */
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], reduceFunc: (VD, VD) => VD): VertexRDD[VD] = {
    def sendMessage(edge: EdgeTriplet[VD, ED]) = {
      val merged = reduceFunc(edge.srcAttr, edge.dstAttr)
      //TODO log println("GraphX message sent " + edge.srcId + " " + edge.dstId + " " + edge.dstAttr + " " +edge.srcAttr + "@@" +(edge.srcAttr == edge.dstAttr))
      if (edge.srcAttr == edge.dstAttr)
        Iterator.empty
      else
        Iterator((edge.dstId, merged), (edge.srcId, merged))
    }
    Pregel[VD, ED, VD](graph, null.asInstanceOf[VD], activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => if (msg == null) attr else reduceFunc(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = reduceFunc).vertices
  }
}
