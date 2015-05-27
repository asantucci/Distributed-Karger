import org.apache.spark.graphx.util.GraphGenerators
//import org.apache.spark._
//import org.apache.spark.graphx._
import org.apache.spark.graphx.{VertexId, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy
//import org.apache.spark.SparkContext._
import org.apache.spark.graphx.GraphOps
import scala.collection.mutable.{ListBuffer, Set, Map, Queue}
import scala.math.log

val N = 10
val M = 10
val mu = 20
val sd = 4

var G = GraphGenerators.logNormalGraph(sc, N, mu, sd)
G.partitionBy(PartitionStrategy.EdgePartition2D) // Think!

val degrees = G.degrees.collect

// Randomly weight edges.
//G.edges.map{e => (e.srcId, e.dstId, (math.random*100).toInt)}

case class OutgoingSingleton(dstVertex: VertexId, weight: Int)
case class OutgoingEdges(srcId: VertexId, 
                         edges: Queue[OutgoingSingleton])

var outgoing = 
        G.edges
         .map(e => (e.srcId, OutgoingSingleton(e.dstId, e.attr)))
         .groupByKey  // No shuffle needed since edges partitioned ?
         .map(p => OutgoingEdges(p._1,
                           Queue(p._2
                                  .toList // use of toList?
                                  .sortBy(_.weight)
                                  .take((log(N)/log(2)).toInt):_*)))
         .collect // send results back to broadcast

// outgoing_mapping of vertexId's to their corresponding pos in outgoing
var outgoing_mapping = Map[Long, Int]()

// Define a table of ConnectedComponents listing the size 
// of our connected components where they can be found in 'g'. 
// Use LBuffer for O(1) pre/append
case class ConnComp(var nodes: Set[VertexId], 
               var count: Int, var indices: Set[Int])
var table_cc = ListBuffer[ConnComp]()
for (i <- 0 to N-1) {
  outgoing_mapping.put(outgoing(i).srcId, i)
  table_cc +=  ConnComp(Set(outgoing(i).srcId), 1, Set(i))
}

var mst = ListBuffer[(Edge[Int])]()
var mst_nodes = Set[Long]()
var num_cc = N
while (table_cc.length > 0 && num_cc > 1) {
  val indices = table_cc(0).indices
  var min = Double.PositiveInfinity
  var to_dequeue = -1
  println(indices)
  for (i <- indices) { // For each V, check lowest outgoing edge...
    if (outgoing(i).edges.head.weight < min) {
      min = outgoing(i).edges.head.weight   // update min
      to_dequeue = i                        // update index
    }
  }
  // Check to ensure we are truly growing the MST.
  val srcId = outgoing(to_dequeue).srcId
  var out = outgoing(to_dequeue).edges.dequeue
  if (!(mst_nodes.contains(srcId) &&
        mst_nodes.contains(out.dstVertex))) {
    mst +=   Edge(srcId, out.dstVertex, out.weight)
    mst_nodes += (srcId, out.dstVertex)
    num_cc -= 1
  }
  var tmp = table_cc.remove(0)
  // Add dstVertex and corresponding pos in cc mapping.
  if (!outgoing(to_dequeue).edges.isEmpty) {
    tmp.nodes   += out.dstVertex
    tmp.indices += outgoing_mapping.getOrElse(out.dstVertex, -1)
    tmp.count += 1
    table_cc  += tmp
  }
}

// Only chuck if list is exhausted AND degree of node > log(n)


// One more map reduce to check if there are any edges leaving connected components which make it possible to connect separate connected components.
