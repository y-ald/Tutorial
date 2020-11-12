import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Spark_Struct_Stream {

  def main(args:Array[String]) = {
    print("dedfe")
    val spark = SparkSession
      .builder
      .appName("SparkStructStream")
      .master("local[*]")
      .getOrCreate()

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ":9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ":9092")
      .option("checkpointLocation", "checkpointLocation-kafka2console")
      .option("topic", "number")
      .start

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start


  }
}
