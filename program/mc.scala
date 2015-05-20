import org.apache.spark.graphx.util.GraphGenerators
//import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner

val N = 10
val M = 10
val B = 10
val mu = 2
val sd = 1.5

def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 < b._2) a else b
}

def concatId(a: List[VertexId], b: List[VertexId]): List[VertexId] = {
    a ++ b
}

class customPartitioner extends Partitioner {
  def numPartitions = B
  def getPartition(key: Any): Int = key.asInstanceOf[Int]
  override def equals(other: Any): Boolean = 
    other.isInstanceOf[customPartitioner]
}

var G = GraphGenerators.logNormalGraph(sc, N, mu, sd)

// sample edges with probability 1/e --> 1/c, where c ~ max degree.
// To Do: Check bounds of P (e.g. what if 1?)
val p_est: Double = 1.0 / G.degrees.reduce(min)._2
//val bc = sc.broadcast(p_est)
val v  = sc.broadcast(G.vertices)

var sampled_edges: RDD[(Int, Edge[Int])] = sc.emptyRDD
for (i <- 1 to B) {
  sampled_edges = sampled_edges.union(
                    G.edges
                     .sample(false, p_est, i) // use of seed?
                     .map(e => (i, e))
                  )
}

sampled_edges = sampled_edges.partitionBy(new customPartitioner)
sampled_edges.reduceByKey(

// 0. Broadcast vertices to all machines.
// 1. Create a RDD with key=machine.no, val = edge
// 2. AggregateUsingIndex - Some function that takes a set of Edges
//    creates sub-graph, runs CC, returns sorted CC-components...

// Option 1: Create (Cluster, Subgraph) RDD, map(CC)
// Option 2: Iteratively: CC(create(Subgraph))



// Perform a Connected Components Analysis "on each machine"
// To do this, generate B subgraphs, perform CC analysis,
// keep track of cc's in subgraphs, and append to master RDD.
var ccs: RDD[List[VertexId]] = sc.emptyRDD
for (i <- 1 to B) {
  var cc = G.subgraph(epred = (e) => // Dislike making a whole variable
                      {scala.util.Random.nextFloat < bc.value })
            .connectedComponents
            .vertices
  cc.cache()
  ccs = ccs.union(
         cc.aggregateUsingIndex(cc.map(pair => (pair._2, List(pair._1))), 
                                concatId)
           .map(p => p._2.sortWith(_ < _))) // can use(p._1, p._2.sortWith...) to track cc id.
}

vertices
    .map(pair => (pair._2, List(pair._1)))
    .aggregateUsingIndex(
            .vertices //   .mapVertices(pair => (pair._2, pair._1))?
  
            .map(pair => (pair._2, List(pair._1)))
   .groupBy
   .reduceByKey(combineLists)
   .groupBy(pair => pair._2)
   .map(pair => (pair._2, pair._1)) // .keyBy(pair => pair._2)   
   .reduce(paste)
   .collect
}

// Sample a set of edges to create sub-graphs.
var d: RDD[(Int, (org.apache.spark.graphx.VertexId,
                  org.apache.spark.graphx.VertexId))] = sc.emptyRDD
for (i <- 1 to B) {
  d = d.union(
        G.edges
         .sample(false, p_est, i)  // use of seed?
         .map(e => (i, (e.srcId, e.dstId)))
  )
}

var s = ConnectedComponents.run(G)

G.edges.map{e => (e, 1)}
G.edges = G.edges.map(e => (e, ()))
for (i <- 1 to B) {
  G.edges = G.edges.map(e => (e.
}

// Create an RDD of graphs?
var t: RDD[(Int, org.apache.spark.graphx.Graph[Int, (Int, Int)])] = sc.emptyRDD
for (i <- 1 to B) {
  t = t.union(G
}

//var G = GraphGenerators.rmatGraph(sc, N, M)

