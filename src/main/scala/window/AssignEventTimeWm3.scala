package window

//乱序的话用这个方法, assignAscendingTimestamps有序的话是用这个方法，（有序的话，不用考虑水位线这些乱序的问题）
import bean.TrainAlarm
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration


object AssignEventTimeWm3 {
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

    val lateTag = new OutputTag[TrainAlarm]("late")

    val result = inputStream
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //在这里加上时间，窗口触发后不会立即关闭，会等待迟到数据，1分钟以后才会关闭
      .allowedLateness(Time.minutes(1))
      //如果一分钟以后数据还没到，那就放入侧输出流
      .sideOutputLateData(lateTag)
      .maxBy("temp")

    result.print()
    result.getSideOutput(lateTag).print("late===")

    //虽然还是滑动窗口，但是执行时间会往后晚2s，因为有水位线控制的延时时间

    env.execute()

  }
}
