package edu.knoldus

import java.util
import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object Consumer {
  val serverConsume = "172.17.0.2:9092"
  val propsStringConsumer = new Properties()
  propsStringConsumer.put("bootstrap.servers", serverConsume)
  propsStringConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsStringConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsStringConsumer.put("auto.offset.reset", "latest")
  propsStringConsumer.put("group.id", UUID.randomUUID().toString)
  propsStringConsumer.put("enable.auto.commit", "false")
  val stringConsumer = new KafkaConsumer[String, String](propsStringConsumer)

  val propsByteArrayConsumer = new Properties()
  propsByteArrayConsumer.put("bootstrap.servers", serverConsume)
  propsByteArrayConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsByteArrayConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  propsByteArrayConsumer.put("auto.offset.reset", "latest")
  propsByteArrayConsumer.put("group.id", UUID.randomUUID().toString)
  propsByteArrayConsumer.put("enable.auto.commit", "false")
  val byteArrayConsumer = new KafkaConsumer[String, Array[Byte]](propsByteArrayConsumer)

  def consumerHeader: Int = {
    stringConsumer.subscribe(util.Collections.singletonList("Image_Header"))
    while (true) {
      val record = stringConsumer.poll(5000)
      record.records("Image_Header").forEach(value => {
        Producer.writeToKafka("Image_Header_Remote", value.key(), value.value())
      })
    }
    67
  }

  def consumeImage: Int = {
    while (true) {
      byteArrayConsumer.subscribe(util.Collections.singletonList("Image_Message"))
      val record = byteArrayConsumer.poll(5000)
      record.records("Image_Message").forEach(value => {
        Producer.writeToKafka("Image_Message_Remote", value.key(), value.value())
      })
    }
    45
  }

}

object Producer {
  val serverProduce = "172.17.0.2:9092"
  private val props = new Properties()
  props.put("bootstrap.servers", serverProduce)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer = new KafkaProducer[String, String](props)

  private val byteArrayProps = new Properties()
  byteArrayProps.put("bootstrap.servers", serverProduce)
  byteArrayProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  byteArrayProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  byteArrayProps.put("max.request.size", "1048576")
  private val byteArrayProducer = new KafkaProducer[String, Array[Byte]](byteArrayProps)

  def writeToKafka(topic: String, key: String, json: Array[Byte]): Unit = {
    byteArrayProducer.send(new ProducerRecord[String, Array[Byte]](topic, key, json)).get()
  }


  def writeToKafka(topic: String, key: String, json: String): Unit = {
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, key, json)
    producer.send(record).get()
  }

}

object MainApplication extends App {
  val consumeStrAndProduce = Future(Consumer.consumerHeader)
  val consumeImageAndProduce = Future(Consumer.consumeImage)
val res = for {
  _ <- consumeStrAndProduce
  _ <- consumeImageAndProduce
} yield "Completed"
  Await.result(res, Duration.Inf)
}
