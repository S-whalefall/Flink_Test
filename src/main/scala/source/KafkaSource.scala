package source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092") //ip地址和端口号，kafka端口号是9092
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"wc") //配置groupid

    //kafka需要：主题，groupid，序列化，prorperties    new SimpleStringSchema()是封装好的序列化方法
    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), props)) //点进去FlinkKafkaConsumer09看参数，（）ctrl+alt+b

    inputStream.print()

    env.execute()
    /*
    * flink 对接kafka
    * 后续操作应该是对进行操作inputStream
    * 配置项比较少，就是配置ip端口+用户组，比较简单
    *
    * 重点，一定要会
    * */
  }
}
