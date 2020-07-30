import org.apache.spark._
import org.apache.log4j.{Level, Logger}


object Project2FacebookFriends{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("FacebookFriends").setMaster("local[*]");
    val sc = new SparkContext(conf)

    // Mapper function
    def FacebookMapper(line: String) = {
      val words = line.split(" ")
      val key = words(0)
      val pairs = words.slice(1, words.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }


    // This is a reducer function: grouped by data and key
    def FacebookReducer(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set // Accumulator is used to intersect the data to find mutual friends
    }

    // given input file = facebook_combined.txt
    val file = sc.textFile("/Users/gabriellawillis/Desktop/facebook_combined.txt")

    val results = file.flatMap(FacebookMapper)
      .reduceByKey(FacebookReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} , (${line._2.mkString(" ")})")})

    // Saving output as textfile named FacebookFriendsOutput
    results.coalesce(1).saveAsTextFile("FacebookFriendsOutput")

  }

}