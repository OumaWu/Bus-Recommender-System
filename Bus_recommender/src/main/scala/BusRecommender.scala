
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class MongoConfig(val uri: String, val db: String)

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class Bus_id(val bus_id:Int)

case class Bus_habit(val uid: Int, val bid: Int, val times: Double, val timestamp: Int)

case class Recommendation(rid:Int, r:Double)

case class passengerRecs(uid:Int, recs:Seq[Recommendation])

case class BusRecs(uid:Int, recs:Seq[Recommendation])

object BusRecommender {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://master:27017/Bus_recommender",
      "mongo.db" -> "Bus_recommender"
    )

    val Bus_recommender_Collection = "Bus_reccomender"
    val Bus_ID = "Bus_id"
    val Bus_Passenger_Base = "Bus_Passenger_Base"


    val NUM = 20

    val sparkConf = new SparkConf().setAppName("Bus_Recommender").setMaster(config("spark.cores"))

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    val mongodbConf = MongoConfig(config("mongo.uri"),config("mongo.db"))

//passenger dataset RDD[Int]
    val Bus_habit_RDD = sparkSession
      .read
      .option("uri", mongodbConf.uri)
      .option("collection", Bus_recommender_Collection)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Bus_habit]
      .rdd
      .map(times =>
        (times.uid, times.bid, times.times ))

    val passenger = Bus_habit_RDD.map(_._1).distinct()
//Bus_id dataset RDD[Int]
    val Bus_id  = sparkSession
      .read
      .option("uri", "mongodb://master:27017/recom3")
      .option("collection", "movies")
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid)


  //get training data, as ALS's requirement, we should build a Rating class.
    val trainData = Bus_habit_RDD.map(x=> Rating(x._1, x._2, x._3))

    val (rank, iterations, lamdba) = (50, 10, 0.01)

    val model = ALS.train(trainData, rank, iterations, lamdba)
  //passenger cartesian with Bus_id get many (passenger, Bus_id), we can see this as test dataset
    val passenger_bus = passenger.cartesian(Bus_id)
  //predict result get many Rating class
    val predict_Bus = model.predict(passenger_bus)
  //sort the result, get first 20
    val passenger_Base = predict_Bus.map(rating => (rating.user, (rating.product, rating.rating))) //change the structure
      .groupByKey()  //many (passenger,Iterable(Bus_id:Int,times:Double))
      .map{
        case (passenger, samelarity) =>
          passengerRecs(passenger,samelarity.toList.sortWith(_._2 > _._2)
            .take(NUM).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    passenger_Base.write
      .option("uri", mongodbConf.uri)
      .option("collection", "Bus_Passenger_Base")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.close()
  }
}
