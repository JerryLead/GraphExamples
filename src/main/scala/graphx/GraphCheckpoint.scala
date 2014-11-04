package graphx

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Graph

object GraphCheckpoint {
  def main(args: Array[String]) {

    val sc = new SparkContext("local[2]", "simpleGraph")

    sc.setCheckpointDir("D:\\data\\checkpoint")

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are postdocs
    // val c1 = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    //graph.edges.partitionsRDD.checkpoint

    // Count all the edges where src > dst
    val finalRDD = graph.edges.filter(e => e.srcId < e.dstId)

    val c= finalRDD.count

    println(c)
    println(finalRDD.toDebugString)


  }
}

