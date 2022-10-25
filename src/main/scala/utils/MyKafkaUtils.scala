package utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object MyKafkaUtils {

  val brokerList = "master:9092,slave1:9092,slave2:9092"

  //获取kafkaSink
  def getKafkaProducer(topic:String) ={
    new FlinkKafkaProducer[String](brokerList,topic,new SimpleStringSchema())   //最后一个参数是序列化的意思，直接放SimpleStringSchema的时候没有报错，后面可以自己理解一下

  }

  //获取kafkaSource
  def getKafkaConsumer(topic:String,groupId:String)={
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop)
  }

}
