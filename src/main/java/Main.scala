import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.lib.{MyTrianglesCounter, TriangleCount}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Triangles").setMaster("local[*]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val verticeCount = TriangleCount.run(graph).vertices

    val tripledTriangles = verticeCount.map(_._2).reduce(_ + _)

    val myImplTriangCount = MyTrianglesCounter.runAndCalculateNumberOfTriangles(graph)
    System.out.println("=============================================================================")
    System.out.println("Number of triangles = " + tripledTriangles/3)
    System.out.println("=============================================================================")
    System.out.println("My wrapped implementation returns number of triangles = " + myImplTriangCount)
    System.out.println("=============================================================================")
 }
}