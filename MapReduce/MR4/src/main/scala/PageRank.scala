import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

// A PageRank class that performs the Spark Job to compute PageRank for given data set for ten iterations
// and returns topK Results
object PageRank {

  // A function that accepts the given Adjacency list as a String, removes delimited commas, surrounding brackets
  // and returns a list
	def makeList(field : String) : List[String] = {
	    if(field.substring(1, field.length()-1).length() ==0)
	      return List[String]()
	    else
			return field.substring(1, field.length()-1).split(", ").toList
	}

  // A Main function that computes Page rank and gives TopK Results
	def main(args: Array[String]): Unit = {

      // Initialize, connect to cluster URL ("local" - standalone)
			val conf = new SparkConf().setMaster(args(0)).setAppName("My App").set("spark.default.parallelism", "20")
			val sc = new SparkContext(conf)

      // Accepts the input data set, parses each line (by applying WIkiBz2Parser written in java)
      // to retrieve page name and its outlinks
			val pairRDD = sc.textFile(args(1), 20).mapPartitions{ (iterator) =>
			iterator.map(line => WikiBz2Parser.performParse(line))
			.filter(line => line!=null)
			.map(line => line.split("~"))
			.map(fields => (fields(0), makeList(fields(1))))}.cache()

      // Retrieves the total number of nodes in the graph structure
			var totalNodes = pairRDD.count()

      // Creates a new pair RDD that initializes a pagerank of 1/totalNodes to every node in the graph structure
			var pageranks = pairRDD.mapValues( v => 1.0/totalNodes)
      // Initializes the value of delta
			var delta = 0.0;

      // Runs the pagerank algorithm for ten iterations
			for (i <- 1 to 10) {
        // creates a pair RDD where every node sends along its contribution to the nodes in the adjacency list
        // forming a RDD of type (String, Double)
				val contribs = pairRDD.join(pageranks).values      //joins two RDDs and flatMaps on its values
						.flatMap{ case (links, rank) =>
						val size = links.size
						if (size ==0) {
                // adds the page rank values of dangling nodes
						    delta = delta + rank
						    None
						}
						else
            // Performs pagerank/size on each outlinks and creates a tuple (URL, contribution)
						links.map(dest => (dest, rank / size))
				}
        // Performs a reducebyKey operation on the newly created RDD above and then applies the
        // PageRank formula to compute a new page rank value for that particular iteration
				var newpageranks = contribs.reduceByKey((x, y) => x + y)
				.mapValues(v => 0.15/totalNodes + 0.85 * ((delta / totalNodes) + v))
        // Subtracts the old page ranks with the newly computed pageranks to obtain the difference of the two values
				var temppageranks = pageranks.subtractByKey(newpageranks)
				.mapValues(x => 0.15/totalNodes + 0.85 * (delta/totalNodes))
        // Performs a Union on both the RDDs so that no nodes are lost in the process
				pageranks = temppageranks.union(newpageranks)
			}
      // Gives out Top100 Results in decreasing order and saves it as a text file
			sc.parallelize(pageranks.sortBy(v => v._2, false).take(100), 1).saveAsTextFile(args(2))
			sc.stop()

	}
}