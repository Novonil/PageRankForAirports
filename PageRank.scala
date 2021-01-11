import org.apache.spark.sql.SparkSession

object PageRank {
  	def main(args: Array[String]): Unit = {
    	if (args.length != 3) {
      		println("Usage: PageRank InputDirWithFileName NumberOfIterations OutputDir")
    }


    val inputFilePath = args(0)
    val numberOfIterations = args(1).toInt
    val outputFilePath = args(2)
    val initialRank = 10.0

    // Create a spark session
    val spark = SparkSession
      .builder()
      .appName("Page Rank")
      .getOrCreate()

    // Storing the input file in data variable
    val data = spark
      .read
      .option("header", "true")
      .csv(inputFilePath)
      .select("ORIGIN", "DEST")

    // Create an rdd of origin city and corresponding destination cities as an iterable
    val links = data.rdd.map(x => (x.getAs[String]("ORIGIN"), x.getAs[String]("DEST"))).distinct().groupByKey().cache()
    
    val totalLinks = links.count()
    
    // Initialize ranks with default value 10.0
    var ranks = links.mapValues(x => initialRank)

    for (i <- 1 to numberOfIterations) {
      val contribution = links.join(ranks).values.flatMap { case (cities, rank) =>
        val size = cities.size
        // Distribute the ranks among all the destination cities from that origin city
        cities.map(city => (city, rank / size))
      }
      // Applies the page rank formula to calculate the new rank values after that iteration
      ranks = contribution.reduceByKey(_ + _).mapValues(0.15/totalLinks + 0.85 * _)
    }

    val output = ranks.sortBy(-_._2)
    
    output.coalesce(1).saveAsTextFile(outputFilePath)
	}
}
