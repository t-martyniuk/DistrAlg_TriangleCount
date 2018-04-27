package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.graphx._


object MyTrianglesCounter {

  def runAndCalculateNumberOfTriangles[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Int = {
    val canonicalGraph = graph.mapEdges(e => true).removeSelfEdges().convertToCanonicalEdges()
    val counters = runPreCanonicalized(canonicalGraph).vertices
    counters.map(_._2).reduce(_ + _)/3
  }














  def runPreCanonicalized[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
    val nbrSets: VertexRDD[VertexSet] =
      graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.length) {
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }

    val setGraph: Graph[VertexSet, ED] = graph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]) {
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }

    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)
    graph.outerJoinVertices(counters) { (_, _, optCounter: Option[Int]) =>
      val dblCount = optCounter.getOrElse(0)
      require(dblCount % 2 == 0, "Triangle count resulted in an invalid number of triangles.")
      dblCount / 2
    }
  }
}
