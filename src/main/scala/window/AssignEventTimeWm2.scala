package window

import bean.TrainAlarm
import org.apache.flink.api.common.eventtime.{TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
* 自定义实现eventTime和Watermark的指定
* */
object AssignEventTimeWm2 {
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
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .maxBy("temp")
      .print()

    env.execute()
  }
}

//自定义实现Watermark的计算
class AssignTsAndWm extends WatermarkStrategy[TrainAlarm]{

  //这个函数的作用是计算watermark
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[TrainAlarm] = {
    new WatermarkGenerator[TrainAlarm] {

  // 在外部定义一个最大延迟时长,和最大时间戳
      val maxOutOfOrderness = 2L
      var maxTimestamp:Long = 0

      override def onEvent(event: TrainAlarm, eventTimestamp: Long, output: WatermarkOutput): Unit = {
        //提取已经来的时间中，最大的eventTime
        maxTimestamp = Math.max(maxTimestamp,eventTimestamp)  //相比的时候要统一数据类型，都调整为Long
      }

      override def onPeriodicEmit(output: WatermarkOutput): Unit = {
        //创建Watermark
        output.emitWatermark(new Watermark(maxTimestamp - maxOutOfOrderness))

      }
    }

  }

  //这个重写方法是指定eventTime
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[TrainAlarm] = {

    new TimestampAssigner[TrainAlarm] {
      override def extractTimestamp(element: TrainAlarm, recordTimestamp: Long): Long = element.ts * 1000L   //传入的是TrainAlarm变量，然后获取他的ts做为eventTime
    }
  }
}

/*
* 技巧，缺什么，直接new一个返回类型的函数
* 比如缺TimestampAssigner，就new 一个 new TimestampAssigner
*
*
* */