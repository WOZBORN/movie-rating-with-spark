import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {
  val spark = SparkSession.builder()
    .appName("Movie Ratings Analysis")
    .master("local[*]")
    .getOrCreate()

  val ratings = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/ratings.csv")

  val avgRatings = ratings.groupBy("movieId")
    .agg(avg("rating").alias("avg_rating"))
    .orderBy(desc("avg_rating"))

  val topRated = avgRatings.join(ratings.groupBy("movieId")
    .count()
    .filter(col("count") >= 50)
    .select("movieId"), "movieId")
    .orderBy(desc("avg_rating"))

  val tags = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/tags.csv")

  val popularTags = tags.groupBy("tag")
    .count()
    .orderBy(desc("count"))

  topRated.write
    .option("header", "true")
    .csv("data/top-rated-movies.csv")

  popularTags.write
    .option("header", "true")
    .csv("data/popular-tags.csv")
}