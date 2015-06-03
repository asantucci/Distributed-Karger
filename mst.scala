/* Authors: Andreas Santucci, Eric Lax
 * Program: Minimum Spanning Tree
 * ------------------------------
 * This program creates function definitions
 * which are used to find a minimum spanning tree
 * for a connected graph, G. In the even that G
 * not completely connected, then the set of connected
 * components are returned.
 */

/* With generous thanks to Swaroop Ramaswamy and Rohit Patki
 * Citation: https://github.com/s-ramaswamy/CME-323-project 
 */

:load ../../../project/program/disjoint_sets.scala  // Source code contained in citation above.

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{VertexId, Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.GraphOps
import scala.collection.mutable.{HashSet, Queue}
import org.apache.spark.util.collection.CompactBuffer
import scala.math.log

/* Class: OutgoingEdge
 * -------------------
 * Case class defining an 'outgoing edge' from a node.
 * Stores the destination vertex id and the edge weight.
 */
case class OutgoingEdge(val dstId: VertexId, val weight: Int)

/* Function: minWeight
 * -------------------
 * minWeight is a simple wrapper function which takes in
 * two edges and returns the edge with minimum weight.
 */
def minWeight(e1: Edge[Int], e2: Edge[Int]): Edge[Int] = {
  if (e1.attr < e2.attr)
    return e1
  return e2
}

/* Function: findSmallestEdge
 * --------------------------
 * findSmallestEdge is a function which facilitates finding
 * the smallest edge leaving a node. The inputs are a vertex
 * id corresponding to the node itself, an iterable object of
 * OutgoingEdge's, and a disjoint set data structure which describes
 * the set of connected components. The function iterates over
 * all pertinent edges and checks to see whether (1) the edge
 * weight is the lowest observed so far in O(1) time and (2)
 * that the edge in fact leaves the connected component in
 * O(lg(n)) time. The lowest weight outgoing edge is returned.
 */
def findSmallestEdge(srcId: VertexId,
                     edges: Iterable[OutgoingEdge],
                     dset: DisjointSet[Long]): Edge[Int] = {
  var min_weight: Int = Int.MaxValue;
  var dstId: Long = -1;
  for (e <- edges) {
      if (e.weight < min_weight &&                    // Lowest weight.
          (dset.find(srcId) != dset.find(e.dstId))) { // Leaving CC.
      dstId = e.dstId;
      min_weight = e.weight;
    }
  }
  return Edge(srcId, dstId, min_weight)
}

/* Function: checkAnyOutgoing
 * --------------------------
 * checkAnyOutgoing is a simple wrapper which checks whether
 * at least one node returned from findSmallestEdge truly
 * leaves a connected component.
 */
def checkAnyOutgoing(edges: Array[Edge[Int]]): Boolean = {
  for (e <- edges) if (e.dstId != -1) return true
  return false
}

/* Function: readEdges
 * -------------------
 * Simple function which facilitates reading in
 * a file of edge weights into an RDD. File is expected
 * to be space delimited, stored in the form:
 * source_node, destination_node, edge_weight.
 */
def readEdges(fname: String): RDD[Edge[Int]] = {
  return sc.textFile(fname).
            map{line => line.split(" ")}.
            map{e => Edge(e(0).toLong, e(1).toLong, e(2).toInt)}
}

/* Function: readVertices
 * ----------------------
 * readVertices is a simple function which reads in
 * a file describing vertices of a graph. It's expected
 * to be space delimited, where the first column contains
 * the vertex ID, and the second the degree of the vertex.
 */
def readVertices(fname: String): RDD[(VertexId, Int)] = {
  return sc.textFile(fname).
            map{line => line.split(" ")}.
            map{v => (v(0).toLong, v(1).toInt)}
}


/* Function: findMST
 * ---------------
 * findMST is the main workhorse of this program. It finds
 * the minimum spanning tree of a graph. The basic algorithm
 * is to start by treating each node as its own connected component.
 * Note that since this function is utilized by Distributed Karger,
 * the edges are already partitioned such that all the edges corresponding
 * to a single node are stored on one machine. The initial groupByKey
 * therefore does not involve a shuffle. Connected components are broadcast
 * out to all machines with shuffle size O(Bn). Finding the lowest
 * weight vertex leaving each connected component for each node costs
 * O(m/B) run time. The results are sent back to the driver with
 * shuffle size O(n) and the lowest weight edge leaving each connected
 * component is found in O(n) time as well. If there are any edges
 * leaving connected components, the connected components are joined
 * in O(lg(c)) time, where 'c' in this case is the size of the connected
 * component.
 * Total run time: O(m/b + n lg(n))
 */

def findMST( G: Graph[Int, Int] ): 
  ( HashSet[Edge[Int]],
    DisjointSet[VertexId] ) = {
  
  var mst = new HashSet[Edge[Int]]  // MST is O(n), can be stored locally.
  var ccs = new DisjointSet[VertexId]

  val N = G.vertices.collect.length
  // Each node starts as its own connected component.
  for (v <- G.vertices.collect) {
    ccs.add(v._1)  
  }

  // Create a list of edges leaving each component.
  var outgoing_edges = 
        G.edges.
          map(e => e.srcId -> OutgoingEdge(e.dstId, e.attr)).
          groupByKey

  // Broadcast out connected components to all machines.
  var ccs_bc = sc.broadcast(ccs)
  
  // Find the lowest weight edge leaving each connected component.
  var to_continue = true
  for (i <- 1 to (log(N)/log(2)).ceil.toInt if to_continue) {
      var to_add = 
        outgoing_edges.map{case(srcId, edges) =>
                       ccs_bc.value.find(srcId) -> 
                       findSmallestEdge(srcId, edges, ccs_bc.value)
        }.reduceByKey{minWeight(_, _)}.
          map(e => e._2).
          collect

      if (!checkAnyOutgoing(to_add)) {
        to_continue = false
      } else {
        for (e <- to_add) {
          if (e.srcId != -1 && e.dstId != -1) {
            mst += e
            ccs.union(e.srcId, e.dstId)
          }
        }
      }
     
      // Broadcast connected components to all machines.
      ccs_bc = sc.broadcast(ccs)
  }

  // Return the MST and a set of connected components.
  return (mst, ccs)
}
