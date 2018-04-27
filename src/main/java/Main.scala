import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Triangles").setMaster("local[*]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    val triangCount = graph.triangleCount().vertices
    val tripledTriangles = triangCount.map(_._2).reduce(_ + _)
    System.out.println("Number of triangles = " + tripledTriangles/3)
 }
}