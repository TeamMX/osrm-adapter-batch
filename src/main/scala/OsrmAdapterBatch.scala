import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

object OsrmAdapterBatch {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.print("Usage: OsrmAdapterBatch <from mongo uri> <from mongo 2 uri> <to csv path> <bucket>")
      System.exit(1)
    }
    val Array(mongouri, mongouri2, csvpath, bucket) = args

    val spark = SparkSession
      .builder
      .appName("Osrm Adapter Batch")
      .getOrCreate()
    import spark.implicits._
    
    val df = MongoSpark.load(spark, ReadConfig(Map("uri" -> mongouri)))
    val records = df.map(row => {
      val Array(from, to) = row
        .getString(row.fieldIndex("_id"))
        .split(" ")
      val speed = row.getDouble(row.fieldIndex("value")) / row.getDouble(row.fieldIndex("weight"))
      (from + " " + to, Math.round(speed))
    })
    
    val df2 = MongoSpark.load(spark, ReadConfig(Map("uri" -> mongouri2)))
    val bucketbroadcast = spark.sparkContext.broadcast(bucket)
    val records2 = df2.map(row => {
      val Array(from, to, bucket) = row
        .getString(row.fieldIndex("_id"))
        .split(" ")
      val speed = row.getDouble(row.fieldIndex("speedKph"))
      (from + " " + to, bucket, Math.round(speed))
    })
    .filter(record => record._2 == bucketbroadcast.value)
    .map(record => (record._1, record._3))

    val records3 = records
      .joinWith(records2, records.col("_1") === records2.col("_1"), "full_outer")
      .map {
        case (l, r) => {
          val left = Option(l).getOrElse(r)
          val right = Option(r).getOrElse(l)
          val Array(from, to) = left._1.split(" ")
          (from, to, (left._2 + right._2) / 2)
        }
      }

    records.write.format("csv").save(csvpath)
  }
}
