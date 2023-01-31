package checkpoint

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

import java.lang
import java.util.Properties
/*
* 状态一致性
*如何保证状态一致性：checkpoint+两阶段提交
* */
object End2EndExactlyDemoOnce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"")
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest") //这个参数是决定读取的offset位置的选择

    val kafkaConsumer = new FlinkKafkaConsumer[String]("", new SimpleStringSchema(), props)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true) //主要是offset要去做checkpoint

    //下面这段是设置checkpoints机制，主要是  EXACTLY_ONCE
    env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("hdfs://master:9000/flink/checkpoint")
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    //此处的EXACTLY_ONCE实现两阶段提交
    val props2 = new Properties()
    props2.setProperty(ProducerConfig.ACKS_CONFIG,"-1")
    props2.setProperty(ProducerConfig.RETRIES_CONFIG,"3")
    props2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
    val kafkaProducer = new FlinkKafkaProducer[String]("test1", new KafkaSerializationSchema[String] {
      override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]]("test2", t.getBytes(), t.getBytes("utf-8"))
      }
    }, props2, Semantic.EXACTLY_ONCE)

  }
}
