package table

import bean.TrainAlarm
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object test2 {
  def main(args: Array[String]): Unit = {
    val bsENV = StreamExecutionEnvironment.getExecutionEnvironment
    bsENV.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
//      .inStreamingMode()
      .inBatchMode()
      .build()

//    val tEnv = TableEnvironment.create(settings)
    val tEnv = StreamTableEnvironment.create(settings)

    val dataStream: DataStream[TrainAlarm] = bsENV.readTextFile("D:\\abc\\xxx.txt").map(
      line=>{
        val splits = line.split(",")
        TrainAlarm(splits(0),splits(1).toLong,splits(2).toDouble)
      }
    )

    val table = tEnv.fromDataStream(dataStream,"asd,asdsa,asdasda")
  }
}
