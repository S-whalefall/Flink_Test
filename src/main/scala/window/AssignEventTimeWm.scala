package window

//乱序的话用这个方法, assignAscendingTimestamps有序的话是用这个方法，（有序的话，不用考虑水位线这些乱序的问题）
import bean.TrainAlarm
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration


object AssignEventTimeWm {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("master", 666)
      .map(line => {
        val splits = line.split(",")
        TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy  //这个方法在flink 1.12后有的
        //计算watermark
        .forBoundedOutOfOrderness(Duration.ofSeconds(2))  //设置延迟时间
        //这一步也是指定eventTime  获取正常的时间并序列化
        .withTimestampAssigner(new SerializableTimestampAssigner[TrainAlarm] { //对TrainAlarm对象里的变量 做序列化设置 ，
          override def extractTimestamp(element: TrainAlarm, recordTimestamp: Long): Long = element.ts * 1000L //变成毫秒相比较
        })
    )

    inputStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .maxBy("temp")
      .print()
    //虽然还是滑动窗口，但是执行时间会往后晚2s，因为有水位线控制的延时时间

    env.execute()

  }
}
