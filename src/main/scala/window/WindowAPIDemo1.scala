package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/*
* 滑动的2秒计算一次，但计算的是从现在，包括现在这条数据往前数10s内的数据。
* 滚Tumbling ，滑Slideing 会话Session
* */

object WindowAPIDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.socketTextStream("master",port = 521)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      //传入一个参数就是滚动窗口，不是进来5条数据，而是相同key的数据来5条后才会触发
//      .countWindow(5)

      //传入两个参数就是滑动窗口，第二个是滑动的步长，只要步长满足数据就可以输出
//      .countWindow(5,2)
//      .sum(1)
//      .print()

      //滚动的时间窗口，5秒内的
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

      //滑动的时间窗口，步长为2秒,
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)))
//      .sum(1)
//      .print()

      //这个是算时间间隔在设定时间内的
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .sum(5)
      .print()


    env.execute()
  }
}
