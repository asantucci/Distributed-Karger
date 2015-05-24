import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.GraphOps
//import scala.util.Sorting

val N = 100
val M = 100
val mu = 20
val sd = 4

var G = GraphGenerators.logNormalGraph(sc, N, mu, sd)
G.partitionBy(PartitionStrategy.EdgePartition2D) // Think!

// Doesn't sorting require (max.deg log(max.deg)) time?
//G.collectNeighborIds(EdgeDirection.Out)

//G.edges.flatMap{e => e.map(e.srcId -> e.attr)}

//case class EdgeWeight(dst: org.apache.spark.graphx.VertexId, wt: Int)
//case class EdgeWeight(arr: Iterable[(org.apache.spark.graphx.VertexId, Int)]) {
//  val edgeOrdering = Ordering.by {tuple => tuple._2}
//}

// Randomly weight edges.
//G.edges.map{e => (e.srcId, e.dstId, (math.random*100).toInt)}

var g = G.edges
         .map(e => (e.srcId, (e.dstId, e.attr)))
         .groupByKey  // No shuffle needed since edges partitioned ?
         .map(pair => pair._1 ->
              scala.collection.mutable.Queue(
                pair._2
                    .toList
                    .sortBy{_._2}
                    .take((math.log(N)/math.log(2)).toInt):_*)) // use of toList?
         .collect // send back to driver...

//def remove(torm:      (org.apache.spark.graphx.VertexId, Int),
//           list: List[(org.apache.spark.graphx.VertexId, Int)]) = 
//  list diff List(torm)

//var mst = new Array[(Edge[Int])]((math.log(N)/math.log(2)).toInt + 1) // truncation?

var mst = new Array[(Edge[Int])](N-1)
var cc  = 0
while (cc < N-1) {
  var min = Double.PositiveInfinity
  var idx = -1
  for (i <- 0 to g.length-1) {
    if (!g(i)._2.isEmpty && 
         g(i)._2.head._2 < min) { // ith element, list element 0, item 2
      min = g(i)._2.head._2
      idx = i
    }
  }
  var tmp = g(idx)._2.dequeue
  mst(cc) = new Edge(g(idx)._1, tmp._1, tmp._2)
  cc += 1
}

////////////////////
 .map{case (src, CompactBuffer(dst, wt)) => 
      src -> _2.sortBy(_.wt)}

 .map{case (src, EdgeWeight) => src -> EdgeWeight.sortBy(._wt).take(3)}
 .map{case (src, EdgeWeight) => src -> 
      EdgeWeight.toList.sortBy(_.wt).take(3)}

G.edges.takeOrdered(Ordering.by([Edge[Int]])._attr)


