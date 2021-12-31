package ovh_test

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Test {
  def main(args:Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("SparkStructStream")
      .master("local[*]")
      .getOrCreate()
    // avoid spark implicits conversatio EX: Dataframe to Datasets
    import spark.implicits._
    val data : Seq[(String, String)] = Seq(
      ("A",	"Blue"),
      ("A","Red"),
      ("A", "Red"),
      ("A", "Blue"),
      ("A", "Blue"),
      ("B", "Green"),
      ("B", "Purple"),
      ("B", "Purple"),
      ("B", "Green"),
      ("C", "Black"))

    val DF = data.toDF("key", "value")
    //DF.show()

    // Use of Window functions
    val window = Window.partitionBy($"key").orderBy($"count_value".desc, $"value")
    val windowAgg = Window.partitionBy($"key", $"value").orderBy($"value")
    val final_df1 = DF
      .withColumn("count_value", count($"value").over(windowAgg))
      .withColumn("rn", row_number().over(window))
      .where($"rn" === 1)
      .orderBy($"key")
      .drop("rn", "count_value")
    final_df1.show()
    print("***************************************** final df1 ***************************")

    // use join strategy
    val middle_df = DF.groupBy($"key", $"value")
      .agg(count($"value").alias("count_value"))
      .withColumn("max_count_value", max($"count_value").over(window))
      .where(($"count_value"===$"max_count_value"))
      .dropDuplicates("key")
      .orderBy($"key")


    //middle_df.show()
    val final_df2 = DF.distinct.join(broadcast(middle_df), List("key", "value"))
      .orderBy($"key")
        .groupBy($"key")
        .agg(first($"value").alias("value"))
    final_df2.show()
    print("***************************************** final df2 ***************************")

    // Use SQL Function for couting fields
    val final_df3 = DF.withColumn("count_value", size(collect_list($"value").over(windowAgg)))
      .withColumn("rn", row_number().over(window))
      .where($"rn" === 1)
      .orderBy($"key")
      .drop("rn", "count_value")
    final_df3.show()
    print("********************************* final 3 **************************************")


    //Ordering over struct
    val final_df4 = DF.withColumn("count_value", count($"value").over(windowAgg))
      .distinct()
      .withColumn("value_key_array", struct("key", "count_value"))
      .orderBy("key","value")
      .groupBy($"key")
      .agg(
        max("value_key_array").alias("value_key_array"),
        first("value").alias("value"))
      .orderBy($"key")
      .select($"key", $"value")
    final_df4.show()
    print("********************************* final 4 **************************************")



    // Usising view in spark SQL
    DF.createOrReplaceTempView("dataframe")
    val final_df5 = spark.sql("""
    SELECT b.key,
    b.value
    FROM (
      SELECT key,
      value,
      COUNT(value) OVER (PARTITION BY key, value) as count_value,
      ROW_NUMBER() OVER (PARTITION BY key ORDER BY value) as rn
      FROM dataframe
    ) b
    WHERE b.rn=1
    ORDER BY b.key
    """)
    //final_df5.show()

  }
}