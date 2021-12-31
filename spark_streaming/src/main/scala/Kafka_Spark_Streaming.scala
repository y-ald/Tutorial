import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.ProducerRecord

object Kafka_Spark_Streaming {

  def main(args:Array[String]){

    val conf = new SparkConf().setAppName("SparkKafkaStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(15))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    val data = Array(
      "a","b", "c", "d", "e", "f","g","h","j","k","l",
      "m","n","o","p","q","r","s","t","u","v","w","x","y","z"
    )
    val letterRdd = sc.parallelize(data).map(x => (x, 0L))

    val topicsSet = Set("test")
    val kafkaParams = giveMeKafkaProps(Array("localhost:9092","console-consumer-23162"))
    val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaParams))
    val lines = stream.map(_.value.toString.toLowerCase())
    val words = lines.flatMap(_.split(""))
    val wordCounts = words.map(x => (x, 1L)).transform(rdd => rdd.union(letterRdd))
    val vowelcounts  = wordCounts.filter( y => y._1 == "a" || y._1 == "e" ||y._1 == "i" ||y._1 == "o" ||y._1 == "u").reduceByKey(_ + _)
    val consonantCounts = wordCounts.filter( y => y._1 != "a" && y._1 != " " && y._1 != "e" && y._1 != "i" && y._1 != "o" && y._1 != "u" && !y._1.matches("\\d+")).reduceByKey(_ + _)
    val numberCounts = wordCounts.filter(y => y._1.matches("\\d+")).reduceByKey(_ + _)

    print("*************** first stream ***********************")
    //stream.map(record=>(record.value())).print()
    print("*********************number count**************")

    val stream1 = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Set("test1"), kafkaParams))
    val lines1 = stream1.map(_.value.toLowerCase())
    val words1 = lines1.flatMap(_.split(""))
    val wordCounts1 = words1.map(x => (x, 1L)).transform(rdd => rdd.union(letterRdd))
    val vowelcounts1  = wordCounts1.filter( y => y._1 == "a" || y._1 == "e" ||y._1 == "i" ||y._1 == "o" ||y._1 == "u").reduceByKey(_ + _)
    val consonantCounts1 = wordCounts1.filter( y => y._1 != "a" && y._1 != "e" && y._1 != "i" && y._1 != "o" && y._1 != "u" && !y._1.matches("\\d+")).reduceByKey(_ + _)
    val numberCounts1 = wordCounts1.filter(y => y._1.matches("\\d+")).reduceByKey(_ + _)
    val joinedVowelStream = vowelcounts.join(vowelcounts1)
    val joinedConsonantStream = consonantCounts.join(consonantCounts1)
-v
    wordCounts1.print()
    joinedVowelStream.print()
    println("Consonant stream =======================")
    joinedConsonantStream.print()
    println("Number Stream ==================")
    joinedNumberStream.print()
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val  producer = new KafkaProducer[String,String](props)
    joinedVowelStream.foreachRDD(x=>{
      val record = new ProducerRecord[String,String]("vowel","key",x.collect().toMap.toString())
      producer.send(record)
    })
    joinedConsonantStream.foreachRDD(x=>{
      val records = new ProducerRecord[String,String]("consonant","key",x.collect().toMap.toString())
      producer.send(records)
    })

    joinedNumberStream.foreachRDD(x=>{

      val records = new ProducerRecord[String,String]("number","key",x.collect().toMap.toString())
      producer.send(records)
    })
    
    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * Function to prepare Kafka params for consuming.
   *
   *
   */
  def giveMeKafkaProps(params:Array[String]) : Map[String, Object] ={

    val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> params(0),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.GROUP_ID_CONFIG -> params(1),
    )

    return kafkaParams

  }

}

