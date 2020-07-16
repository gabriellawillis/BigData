import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

object SparkSQL {
  def main(args: Array[String]): Unit = {



    //    Create SparkSession and SQLContext
    val sparkSession = SparkSession.builder().appName("Spark SQL basic example").master("local[*]").getOrCreate()
    val SQLContext = sparkSession.sqlContext




    //PART_1


    //  1.	Import the dataset and create data frames directly on import.
    val df = SQLContext.read.option("header", "true").csv("/Users/gabriellawillis/Desktop/ICP10Data/survey.csv")
    //   To show the first 20 records
    // df.show(20)


    //  2.	Save data to file.
    //df.write.mode("overwrite").option("header","true").csv("output")


    //  3.	Check for Duplicate records in the dataset
    //val distinctDF = df.distinct()
    //println("Total # of records : "+ df.count()+ "\tDistinct count: "+distinctDF.count())


    //    4.	Apply Union operation on the dataset and order the output by Country Name alphabetically.
    val femaleDF = df.filter("Gender LIKE 'f%' OR Gender LIKE 'F%' ")
    val maleDF   = df.filter("Gender LIKE 'm%' OR Gender LIKE 'M%' ")
    //val records = maleDF.union(femaleDF).orderBy("Country")
    //records.show(100)


    //    5.	Use Groupby Query based on treatment.
    //df.groupBy("treatment").count().show(10)



    //PART_2
    // 1.Apply the basic queries related to Joins and aggregate functions (at least 2)
    //Inner join female and male age and country where country are the same.
    val df1 = femaleDF.select("Age" ,"State")
    val df2 = maleDF.select("Age" ,"State")
    //val jointdf = df1.join(df2, df1("State") === df2("State"), "inner")
    //jointdf.show(false)


    //average age in each country
    //val udf = df2.union(df1)
    //val uniondf =udf.withColumn("Age", udf.col("Age").cast(DataTypes.IntegerType))
    //uniondf.groupBy("State").mean("Age").show()


    //    2.	Write a query to fetch 13th Row in the dataset.
    //println(df.take(13).last)

  }

}