package window

import bean.TrainAlarm
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object WindowEventTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092") //ip地址和端口号，kafka端口号是9092
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"wc") //配置groupid

    //kafka需要：主题，groupid，序列化，prorperties    new SimpleStringSchema()是封装好的序列化方法
    val inputStream = env.socketTextStream("master",666)
    .map(line =>{{
      val splits = line.split(" ")
      TrainAlarm(splits(0),splits(1).toLong,splits(2).toDouble)
    }})
    //如果读取到数据是有序的并且升序的，那么就用下面的这种  id和ts都是TrainAlarm里面的变量
      .assignAscendingTimestamps(_.ts * 1000L)
      .keyBy(_.id)

//    inputStream
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .maxBy("temp")
//      .print()

//    inputStream
//      .window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
//      .maxBy("temp")
//      .print()

    inputStream
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
      .maxBy("temp")
      .print()


/*
验证数据 nc -lk 666
train_1,1547718199,35.8
train_2,1547718200,35.9
train_2,1547718201,35.9
train_2,1547718202,36.1
train_3,1547718204,36.2
train_6,1547718205,36.5
train_7,1547718206,35.8
train_1,1547718207,35.9
train_2,1547718208,36.7
train_1,1547718210,38.1
train_1,1547718211,38.1
train_2,1547718213,35.9*/

  }
}
