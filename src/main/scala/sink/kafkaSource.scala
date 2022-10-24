package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

object kafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val result = env.readTextFile("")

    result.addSink(new FlinkKafkaProducer09[String]("master:9092","test",new SimpleStringSchema()))
    //这个相当于是作为kafka的生产者往集群这个端口发数据，kafka在那个端口弄个消费者可以看


    env.execute()
  }
}
