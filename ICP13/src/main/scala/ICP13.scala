import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object ICP13 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("ICP13")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("ALGORITHMS")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/gabriellawillis/Desktop/Datasets/trip.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/gabriellawillis/Desktop/Datasets/station.csv")



    // Printing the Schema
    println("1. Import Data ")

    //trips_df.printSchema()
    //station_df.printSchema()


    trips_df.createOrReplaceTempView("Trips")

    station_df.createOrReplaceTempView("Stations")


    val nstation = spark.sql("select * from Stations")

    val ntrips = spark.sql("select * from Trips")

    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")


    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()
    stationGraph.vertices.show(10)
    stationGraph.edges.show(10)


    // Triangle Count
    println("2. Triangle Count ")
    val stationTraingleCount = stationGraph.triangleCount.run()
    stationTraingleCount.select("id","count").show(10)

    // Shortest Path
    println("3. Shortest Path ")
    val shortPath = stationGraph.shortestPaths.landmarks(Seq("Golden Gate at Polk","MLK Library")).run
    shortPath.show(10)

    //Page Rank
    println("4. Page Rank ")
    val stationPageRank = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.select("id", "pagerank").show(10)


    // BFS
    println("5. Save Graphs")
    val pathBFS = stationGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 10").run()
    //pathBFS.show(10)

    //Saving to File
    stationGraph.vertices.write.csv("/Users/gabriellawillis/Desktop/1.csv")

    stationGraph.edges.write.csv("/Users/gabriellawillis/Desktop/2.csv")


  }

}