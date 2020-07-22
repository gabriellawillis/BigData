import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

object SparkGraphFrame {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("GraphFrameApplication").master("local[*]").getOrCreate()

    // 1.	Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.
    val station_df = spark.read.format("csv").option("header", true).load("/Users/gabriellawillis/Desktop/Datasets/station.csv")
    //station_df.show(10)

    val trip_df = spark.read.format("csv").option("header", true).load("/Users/gabriellawillis/Desktop/Datasets/trip.csv")
    //trip_df.show(10)

    //  2.	Concatenate chunks into list & convert to Data Frame
    //trip_df.select(concat(col("Start Date"), lit(","), col("End Date")).as("Trip Endpoints")).show(10, false)



    //  3.	Remove duplicates
    val stationDF = station_df.dropDuplicates()
    val tripDF = trip_df.dropDuplicates()



    //    4.	Name Columns
    val renamed_tripDF = tripDF.withColumnRenamed("Trip ID", "tripId")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("Start Station", "StartStation")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Station", "EndStation")
      .withColumnRenamed("End Terminal", "dst")
      .withColumnRenamed("Bike #", "BikeNum")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")

    val rename_stationDF = stationDF.withColumnRenamed("station_id", "StationID")
      .withColumnRenamed("name","Name")
      .withColumnRenamed("lat","Latitude")
      .withColumnRenamed("long","Longitude")
      .withColumnRenamed("dockcount","DockCount")
      .withColumnRenamed("landmark","Landmark")
      .withColumnRenamed("installation","Installation")

    //    5.	Output Data Frame
    //stationDF.show(10, false)
    //renamed_tripDF.show(10, false)


    //    6.	Create vertices
    val vertices = stationDF.select(col("station_id").as("id"),
      col("name"),
      concat(col("lat"), lit(","), col("long")).as("lat_long"),
      col("dockcount"),
      col("landmark"),
      col("installation"))


    val edges = renamed_tripDF.select("src", "dst", "tripId", "StartDate", "StartStation", "EndDate", "EndStation", "BikeNum", "SubscriberType", "ZipCode")
    //edges.show(10, false)



    //    7.	Show some vertices
    val g = GraphFrame(vertices, edges)
    //g.vertices.select("*").orderBy("landmark").show()




    //    8.	Show some edges
    //g.edges.groupBy("src", "StartStation", "dst", "EndStation").count().orderBy(desc("count")).show(10)



    //    9.	Vertex in-Degree
    val in_Degree = g.inDegrees
    //in_Degree.orderBy(desc("inDegree")).show(10, false)



    //    10.	Vertex out-Degree
    val out_Degree = g.outDegrees
    //out_Degree.show(10)
    //vertices.join(out_Degree, Seq("id")).show(10)


    //    11.	Apply the motif findings.
    //val motifs = g.find("(a)-[c]->(d)").show()


    // 12. Stateful Queries
    //g.edges.filter("ZipCode = 94107").show()


    // 13. Subgraphs w/ conditions
    val v2 = g.vertices.filter("dockcount > 20")
    val e2 = g.edges.filter("src > 50")
    val g2 = GraphFrame(v2, e2)
    //g2.vertices.select("*").orderBy("id").show(10)




    //    Bonus
    //    1.Vertex degree
    //g.degrees.show(10)



    //    2. what are the most common destinations in the dataset from location to location.
    //g.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)



    //    4.Save graphs generated to a file.
    //g.vertices.write.mode("overwrite").parquet("output_vertices")
    //g.edges.write.mode("overwrite").parquet("output_edges")



    spark.stop()
  }

}

