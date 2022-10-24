package process

import bean.TrainAlarm
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import window.AssignTsAndWm

object KeyedProcessFuncDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("master", 666)
      .map(line => {
        val splits = line.split(",")
        TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new AssignTsAndWm)
      .keyBy(_.id)
      .process(new MyKeyedProcessFunc)
  }
}

class MyKeyedProcessFunc extends KeyedProcessFunction[String,TrainAlarm,String]{
  override def processElement(i: TrainAlarm, context: KeyedProcessFunction[String, TrainAlarm, String]#Context, collector: Collector[String]): Unit = {
    //每个元素都会调用一次processElement方法
    context.getCurrentKey
    context.timerService().currentWatermark()
    context.timerService().currentProcessingTime()
    context.timerService().registerProcessingTimeTimer(0L)
    context.timerService().deleteEventTimeTimer(0L)
    context.timestamp()
    context.output(new OutputTag[String]("side"),i.id)
    collector.collect("")
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, TrainAlarm, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器实现的方法
    ctx.getCurrentKey
    out.collect("")
  }
}
