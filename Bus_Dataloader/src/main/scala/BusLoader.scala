
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class MongoConfig(val uri: String, val db: String)

case class Bus_habit(val uid: Int, val bid: Int, val times: Double, val timestamp: Int)

object BusLoader {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://master:27017/Bus_recommender",
      "mongo.db" -> "Bus_recommender"
    )

    val BUS_HABIT_FILE_PATH = "/opt/data/Bus_habit.csv"

    val Bus_recommender_Collection = "Bus_reccomender"

    val Bus_id = "Bus_id"
    //create spark config
    val sparkConf = new SparkConf().setAppName("BusdataLoader").setMaster(config("spark.cores"))
    //create spark Session. this is the begin of spark SQL
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //read the text file
    val Bus_habit_RDD = spark.sparkContext.textFile(BUS_HABIT_FILE_PATH)

    val Bus_habit_DF = Bus_habit_RDD.map(line => {
      val x = line.split(",")
      Bus_habit(x(0).toInt, x(1).toInt, x(2).toDouble, x(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    mongoClient(mongoConfig.db)(Bus_recommender_Collection).dropCollection()
    Bus_habit_DF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",Bus_recommender_Collection)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()



    mongoClient.close()
    println("load finished")


  }
}
