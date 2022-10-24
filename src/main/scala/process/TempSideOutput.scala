package process

import bean.TrainAlarm
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.time.Duration

/*
* 温度高于35℃放到测输出流中
* */
object TempSideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val high = new OutputTag[TrainAlarm]("high")

    val result = env.socketTextStream("master", 666)
      .map(line => {
        val splits = line.split(",")
        TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[TrainAlarm] { //extract是提取的意思
          override def extractTimestamp(element: TrainAlarm, recordTimestamp: Long): Long = element.ts * 1000L
        })
    ).process(
      new ProcessFunction[TrainAlarm, TrainAlarm] {
        override def processElement(i: TrainAlarm, context: ProcessFunction[TrainAlarm, TrainAlarm]#Context, collector: Collector[TrainAlarm]): Unit = {
          if (i.temp > 35) {
            context.output(high, i)
          } else {
            collector.collect(i)
          }

        }
      }
    )

    result.getSideOutput(high).print()
    result.print()

    env.execute()
  }
}
