import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.GraphOps
//import scala.util.Sorting

val N = 10
val M = 10
val mu = 2
val sd = 1.5

var G = GraphGenerators.logNormalGraph(sc, N, mu, sd)
G.partitionBy(PartitionStrategy.EdgePartition2D) // Think!

// Doesn't sorting require (max.deg log(max.deg)) time?
//G.collectNeighborIds(EdgeDirection.Out)

//G.edges.flatMap{e => e.map(e.srcId -> e.attr)}

case class EdgeWeight(dst: org.apache.spark.graphx.VertexId, wt: Int)
case class EdgeWeight(arr: Iterable[(org.apache.spark.graphx.VertexId, Int)]) {
  val edgeOrdering = Ordering.by {tuple => tuple._2}
}

G.edges
 .map(e => (e.srcId, (e.dstId, e.attr)))
 .groupByKey  // No shuffle needed since edges partitioned ?
 .map(pair => 
      (pair._1,
       pair._2.toList.sortBy{_._2}.take(10))) // use of toList?




////////////////////
 .map{case (src, CompactBuffer(dst, wt)) => 
      src -> _2.sortBy(_.wt)}

 .map{case (src, EdgeWeight) => src -> EdgeWeight.sortBy(._wt).take(3)}
 .map{case (src, EdgeWeight) => src -> 
      EdgeWeight.toList.sortBy(_.wt).take(3)}

G.edges.takeOrdered(Ordering.by([Edge[Int]])._attr)


