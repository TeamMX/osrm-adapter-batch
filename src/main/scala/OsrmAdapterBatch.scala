import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

case class SegmentSpeed(segment: String, speed: Long)
case class SegmentBucketSpeed(segment: String, bucket: String, speed: Long)
case class FromToSpeed(from: String, to: String, speed: Long)

object OsrmAdapterBatch {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.print("Usage: OsrmAdapterBatch <from realtime mongo uri> <from batch mongo uri> <to csv path> <bucket>")
      System.exit(1)
    }
    val Array(realtimeMongoUri, batchMongoUri, outputCsvPath, batchBucket) = args

    val spark = SparkSession
      .builder
      .appName("Osrm Adapter Batch")
      .getOrCreate()
    import spark.implicits._
    
    val realtimeDataframe = MongoSpark.load(spark, ReadConfig(Map("uri" -> realtimeMongoUri)))
    val realtimeSpeeds = realtimeDataframe.map(row => {
      val Array(from, to) = row
        .getString(row.fieldIndex("_id"))
        .split(" ")
      val speed = row.getDouble(row.fieldIndex("value")) / row.getDouble(row.fieldIndex("weight"))
      new SegmentSpeed(from + " " + to, Math.round(speed))
    })
    
    val batchDataframe = MongoSpark.load(spark, ReadConfig(Map("uri" -> batchMongoUri)))
    val batchBucketBroadcast = spark.sparkContext.broadcast(batchBucket)
    val batchSpeeds = batchDataframe.map(row => {
      val Array(from, to, bucket) = row
        .getString(row.fieldIndex("_id"))
        .split(" ")
      val speed = row.getDouble(row.fieldIndex("speedKph"))
      new SegmentBucketSpeed(from + " " + to, bucket, Math.round(speed))
    })
    .filter(record => record.bucket == batchBucketBroadcast.value)
    .map(record => new SegmentSpeed(record.segment, record.speed))

    val allSpeeds = realtimeSpeeds
      .joinWith(batchSpeeds, realtimeSpeeds.col("_1") === batchSpeeds.col("_1"), "full_outer")
      .map {
        case (l, r) => {
          // outer join, so we may have nulls
          val left = Option(l).getOrElse(r)
          val right = Option(r).getOrElse(l)
          val Array(from, to) = left.segment.split(" ")
          (from, to, (left.speed + right.speed) / 2)
        }
      }

    allSpeeds.write.format("csv").save(outputCsvPath)
  }
}
