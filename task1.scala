
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object task1 {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .appName("task1")
      .getOrCreate()

    val df = spark.read.json("src/main/scala/Toys_and_Games_5.json").select("asin", "overall")

    //val groupbydata= df.groupBy("asin").agg(avg("overall"))
    val groupbydata= df.groupBy("asin").mean("overall").sort("asin")
    groupbydata.show()
    //df.coalesce(1).write.option("header", "true").csv("src/main/scala/Aayush_Sinha_task1.csv")

  }
}
