# Spark Streaming

In this tutorial i lear how to implement à streaming process with spark and Kafka
For our tutorial we have sent a text file which will be read and streamed by kafka and then sent back to spark for analysis.

### Setting up the environment 
Concerning our working environment the tools mainly used are Apache Spark for information processing and Apache Kafka as message bus.
For the installation of Kafka I followed the official documentation which can be found at https://kafka.apache.org/quickstart 

### Sends data in real time
We use Kafka to send messages. Kafka is a distributed publish-subscribe messaging system that was created as a fast, scalable, and durable alternative to existing solutions. It is designed to broker enormous message streams for extremely low-latency analysis within cloud platforms. 


### Data analysis with spark streaming
Concerning the latter. The aim of this tutorial was to discover and familiarise myself with the use of spark streaming
Instead of processing the streaming data one record at a time, Spark Streaming discretizes the streaming data into tiny, sub-second micro-batches. In other words, Spark Streaming’s Receivers accept data in parallel and buffer it in the memory of Spark’s workers nodes. 
