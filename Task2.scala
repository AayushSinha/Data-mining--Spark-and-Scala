
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

object Task2 {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .appName("Task2")
      .getOrCreate()

    val df1 = spark.read.json("src/main/scala/Toys_and_Games_5.json").select("asin", "overall")
    val df2 = spark.read.json("src/main/scala/metadata.json").select("asin", "brand")
    val df_join = df1.join(df2, df1.col("asin").equalTo(df2.col("asin")))
   // average_rating = df.groupBy("asin").agg(mean("overall").alias("rating_avg")).sort("asin")
    val df = df_join.groupBy("brand").agg(mean("overall").alias("rating_avg")).filter((col("brand").isNotNull)).filter("brand!=''").sort("brand")
    df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").csv("src/main/scala/my_aayush_task_2.csv")
  }
}