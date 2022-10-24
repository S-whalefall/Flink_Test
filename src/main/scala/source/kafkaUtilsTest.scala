package source

import org.apache.flink.streaming.api.scala._
import utils.MyKafkaUtils

object kafkaUtilsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaStream = env.addSource(MyKafkaUtils.getKafkaConsumer("test", "xx")) //kafka集群上的消费者是这边的源

    kafkaStream.map(_.split(" ")(0))  //对Stream流数据进行处理，逻辑自定
      .addSink(MyKafkaUtils.getKafkaProducer("test1"))  //这里是要输出去，作为源输出去

    env.execute()

  }
}
