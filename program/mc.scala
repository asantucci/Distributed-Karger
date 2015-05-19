import org.apache.spark.graphx.util.GraphGenerators
//import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val N = 10
val M = 10
val B = 10
val mu = 2
val sd = 1.5

def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 < b._2) a else b
}


def paste(id: (VertexId, VertexId), l: List[VertexId]): List[VertexId] = {
  l ++ List(id._2)
}

// Option 1: Create (Cluster, Subgraph) RDD, map(CC)
// Option 2: Iteratively: CC(create(Subgraph))

val subGraph: Graph[Int, (Int, Int)]  // Degree, Edge Weight, Cluster
//var G = GraphGenerators.rmatGraph(sc, N, M)
var G = GraphGenerators.logNormalGraph(sc, N, mu, sd)

// sample edges with probability 1/e --> 1/c, where c ~ max degree.
// To Do: Check bounds of P (e.g. what if 1?)
val p_est: Double = 1.0 / G.degrees.reduce(min)._2
val bc = broadcast(p_est)

def aggFun(a: List[VertexId], b: List[VertexId]): List[VertexId] = {
  a ++ b
}

for (i <- 1 to B) {
  var cc = G.subgraph(epred = (e) => // Dislike making a whole variable
                      {scala.util.Random.nextFloat < bc.value })
            .connectedComponents
            .vertices
  cc.cache()
  cc.aggregateUsingIndex(cc.map(pair => (pair._2, List(pair._1))),
                         aggFun)
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

