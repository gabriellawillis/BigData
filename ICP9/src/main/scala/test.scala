object test {
  import org.apache.spark.{SparkConf, SparkContext}

  object DFS {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
      val sc = new SparkContext(conf)
      type Vertex = Int
      type Graph = Map[Vertex, List[Vertex]]
      val g: Graph = Map(1 -> List(2,4), 2-> List(1,3), 3-> List(2), 4-> List(1))
      //example graph meant to represent
      //  1---2
      //  |   |
      //  4---3
      //    def BFS(start: Vertex, g: Graph): List[List[Vertex]] = {
      //
      //      def BFS0(elems: List[Vertex],visited: List[List[Vertex]]): List[List[Vertex]] = {
      //        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
      //        if (newNeighbors.isEmpty)
      //          visited
      //        else
      //          BFS0(newNeighbors, newNeighbors :: visited)
      //      }
      //      BFS0(List(start),List(List(start))).reverse
      //    }
      def DFS(start: Vertex, g: Graph): List[Vertex] = {

        def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
          if (visited.contains(v))
            visited
          else {
            val neighbours:List[Vertex] = g(v) filterNot visited.contains
            neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
          }
        }
        DFS0(start,List()).reverse
      }

      val bfsresult=DFS(2,g)

      println("DFS Output",bfsresult.mkString(","))
    }
  }
}