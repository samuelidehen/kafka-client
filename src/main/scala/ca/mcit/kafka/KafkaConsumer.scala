package ca.mcit.kafka
import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
object KafkaConsumer extends App {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-1")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")

  //Producer for EnrichedTrip
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  val producer = new KafkaProducer[String, String](producerProperties)
  val enrichedTopic = "sam_enriched_trip"

  val consumer = new KafkaConsumer[Int, String](consumerProperties)
  consumer.subscribe(List("sam_test2").asJava)

  println("| Key | Message | Partition | Offset |")
  while (true) {
    val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record = recordIterator.next()
        val p = record.value().split(",", -1)
        val trip = Trip(p(0).toInt, p(1), p(2), p(3), p(4).toInt, p(5).toInt, p(6).toInt,if (p(7).isEmpty) None else Some(p(7)),
          if (p(8).isEmpty) None else Some(p(8)))

        val enrichedTrip = EnrichedTrip(trip, None, None)

        val output = EnrichedTrip.toCsv(enrichedTrip)
        println(output)
        consumer.commitSync(Duration.ofSeconds(1))
        producer.send(new ProducerRecord[String, String](enrichedTopic, output))
        producer.flush()
      }
    }
  }
  producer.close()
}
