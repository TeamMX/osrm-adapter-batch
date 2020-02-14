import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

object OsrmAdapterBatch {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.print("Usage: OsrmAdapterBatch <from mongo uri> <to csv path>")
      System.exit(1)
    }
    val Array(mongouri, csvpath) = args

    val spark = SparkSession
      .builder
      .appName("Osrm Adapter Batch")
      .config("spark.mongodb.input.uri", mongouri)
      .getOrCreate()
    import spark.implicits._
    
    val df = MongoSpark.load(spark)
    val records = df.map(row => {
      val Array(from, to) = row
        .getString(row.fieldIndex("_id"))
        .split(" ")
      val speed = row.getDouble(row.fieldIndex("value")) / row.getDouble(row.fieldIndex("weight"))
      (from, to, speed)
    })
    records.write.format("csv").save(csvpath)
  }
}
