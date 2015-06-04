:load ../../../project/program/mst_low_runtime.scala
import scala.util.Random
import org.apache.spark.graphx.GraphOps

/* Function: max_weight
 * --------------------
 * simple wrapper to compute the maximal edge weight in a pair.
 */
def max_weight(e1: Edge[Int], e2: Edge[Int]): Edge[Int] = {
  if (e1.attr < e2.attr)
    return e2
  return e1
}

/* Function: get_cut_size
 * ----------------------
 * wrapper function which determines if an edge crosses
 * a cut. If it does, the function emits 1, else 0.
 */
def get_cut_size(edge: Edge[Int],
                 ccs: Map[VertexId, VertexId]): Int = {
  if (ccs(edge.srcId) != ccs(edge.dstId))
    return 1
  return 0
}

/* Function: get_cut
 * -----------------
 * Given a Graph and an edge, this function computes
 * the number of edges crossing the cut.
 * Question: map across edges in G or MST?
 */

// Create a graph, G.
val E = readEdges("../../../project/program/graphs/barbell_edges.txt")
val V = readVertices("../../../project/program/graphs/barbell_vertices.txt")
val G = Graph(V, E)


/* Function: min_cut_karger
 * ------------------------
 * min_cut_approximation algorithm. Runs O(n^2 lg(n)) iterations
 * of Karger's contraction algorithm. With each iteration,
 * edges of the graph G are randomly weighted, and an MST is computed
 * in O(m/b + n lg(n)) time. The largest weight edge is then removed,
 * and this is equivalent to a series of contractions in Kargers 
 * algorithm, since we now have a cut. Connected components analysis
 * is computed on the sub-graph to compute the partition of nodes,
 * at which point the value of the sampled cut is determined.
 */
def min_cut_karger(G: Graph[Int, Int]): Double = {

  val N = G.vertices.collect.length
  G.partitionBy(PartitionStrategy.EdgePartition2D)
  var min_cut: Double = Double.PositiveInfinity

  for (i <- 1 to N*N*( (log(N)/log(2)).ceil.toInt) ) {
    val arb_weights = G.edges.map{e => 
      val weight = new Random;
      Edge(e.srcId, e.dstId, weight.nextInt(Int.MaxValue))}
    arb_weights.cache()  // Used to save randomly generated weights.
    val G_arb = Graph(G.vertices, arb_weights)
    val (mst, cc) = findMST(G_arb)
    val maxw = arb_weights.reduce(max_weight)
    val G_contract = new GraphOps(Graph(G.vertices, 
                                        G.edges.filter{e =>
                                        e.srcId != maxw.srcId &&
                                        e.dstId != maxw.dstId}))
    val cc_contract = G_contract.connectedComponents.
                                 vertices.
                                 map{v => v._1 -> v._2}.
                                 collect.
                                 toMap
    val cc_bc = sc.broadcast(cc_contract)
    val maxw_bc = sc.broadcast(maxw)
    val tmp_cut = G.edges.
                    map{get_cut_size(_, cc_bc.value)}.
                    reduce(_ + _)
                  
    if (tmp_cut < min_cut)
      min_cut = tmp_cut
  }

  return min_cut
}
