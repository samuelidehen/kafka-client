package ca.mcit.kafka
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ StringSerializer , IntegerSerializer}
import scala.io.Source

object KafkaProducerTrip extends App {
  // Manual File read for Trips
  val tripSource = Source.fromFile("data/trips.txt")
  val tripList: List[Trip] = tripSource
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Trip(p(0).toInt, p(1), p(2), p(3), p(4).toInt, p(5).toInt, p(6).toInt,
      if (p(7).isEmpty) None else Some(p(7)),
      if (p(8).isEmpty) None else Some(p(8))))
  tripSource.close()

  val topicName = "sam_test2"
  val producerProperties = new Properties()
  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092"
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
  )
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
  )
  val producer = new KafkaProducer[ Int , String](producerProperties)
  tripList.foreach(singleTrip => {
    producer.send(new ProducerRecord[Int,String](topicName,Trip.toCsv(singleTrip) ))
  })

   producer.flush()

}
