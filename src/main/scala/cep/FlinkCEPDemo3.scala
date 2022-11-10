package cep

import bean.UserLogin
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util
/*
* 5s内一个用户连续登录失败两次
*
* */
object FlinkCEPDemo3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.readTextFile("")
      .map(line => {
        val splits = line.split(",")
        UserLogin(splits(0).toLong, splits(1), splits(2), splits(3).toLong)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
          .withTimestampAssigner(new SerializableTimestampAssigner[UserLogin] {
            override def extractTimestamp(t: UserLogin, l: Long): Long = {
              t.ts * 1000L
            }
          })
      )

    val pattern1 = Pattern.begin[UserLogin]("begin")
      .where(_.status == "fail")
//      .next("next").where(_.status == "fail")
      .times(2)   //使用两次，两次模式匹配在5秒内就可以
      .within(Time.seconds(5))

    CEP.pattern(dataStream.keyBy(_.uid),pattern1)
      .process(
        new PatternProcessFunction[UserLogin,String] {
          override def processMatch(map: util.Map[String, util.List[UserLogin]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
            val firstUser = map.get("begin").iterator().next()
            val secondUser = map.get("next").iterator().next()
            collector.collect(firstUser.toString + "   " + secondUser.toString)
          }
        }
      ).print()

    env.execute()
  }
}
