import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Triangles").setMaster("local[*]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    val triangCount = graph.triangleCount().vertices
    println(triangCount.map(_._2).reduce(_ + _))
 }
}