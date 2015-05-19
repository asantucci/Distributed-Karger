import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.rdd.RDD

val N = 100
val M = 100
val B = 10

var G = GraphGenerators.rmatGraph(sc, N, M)

// sample edges with probability 1/e --> 1/c, where c ~ max degree.
val p_est: Double = 1.0/G.degrees.map{case (node, deg) => deg}.min // check bounds?

var d: RDD[(Int, (org.apache.spark.graphx.VertexId,
                  org.apache.spark.graphx.VertexId))] = sc.emptyRDD

for (i <- 1 to B) {
  d = d.union(
        G.edges
         .sample(false, p_est, i)
         .map(e => (i, (e.srcId, e.dstId)))
  )
}

var s = ConnectedComponents.run(G)
