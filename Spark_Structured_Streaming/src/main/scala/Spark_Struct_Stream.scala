import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
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
        import spark.implicits._


        // Subscribe to 1 topic
        val df = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", ":9092")
          .option("subscribe", "test")
          .option("startingOffsets", "earliest")
          .load()
        val df1 = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", ":9092")
          .option("subscribe", "test1")
          .option("startingOffsets", "earliest")
          .load()

        val listVowel = List("a", "e", "i", "o", "u")
        val result2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          /*.withColumn("letter", explode(split($"value", "")))
          .groupBy($"letter")
          .count()*/
          /*.map(x => x._2.split("").toString.toLowerCase())
          .map(x => (x, 1))
          .filter(x => !(listVowel.contains(x._1)))*/
          .writeStream
          .outputMode("append")
          .format("kafka")
          .option("kafka.bootstrap.servers", ":9092")
          .option("checkpointLocation", "checkpointLocation-kafka2console")
          .option("topic", "number")
          //.trigger(Trigger.Once())
          .start()
        val result1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .withColumn("letter", explode(split($"value", "")))
          .groupBy($"letter")
          .count()
          //.map(x => x.map(y => (y, 1)))
          /*.filter(x => !(listVowel.contains(x._1)))*/
          .writeStream
          .outputMode("complete")
          .format("console")
          .trigger(Trigger.Once())
          .start


        /*val result3 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
          /*.map(x => x._2.split("").toString.toLowerCase())
          .map(x => (x, 1))
          .filter(x => !(listVowel.contains(x._1)))*/
          .writeStream
          .outputMode("append")
          .format("json")
          .option("checkpointLocation", "/home/yald/Documents/Training/Tutorial/Spark_Structured_Streaming/dir")
          .option("path", "/home/yald/Documents/Training/Tutorial/Spark_Structured_Streaming/data")
          .start()*/

        result2.awaitTermination()
        result1.awaitTermination()
        /*result3.awaitTermination()*/

    }



}
