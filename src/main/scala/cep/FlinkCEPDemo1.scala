package cep

import bean.UserLogin
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala._

import java.time.Duration
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.util.Collector

import java.util


object FlinkCEPDemo1 {
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

    //定义一个模式Pattern
    val pattern1 = Pattern.begin[UserLogin]("begin")
      //    Pattern.begin[UserLogin]("begin",AfterMatchSkipStrategy.noSkip())       //跳过策略
      .where(_.status == "fail").where(_.ip.startsWith("192"))
      .oneOrMore
      .where(_.status == "fail")
      .where(new IterativeCondition[UserLogin] {   //像是自定义的过滤条件
        override def filter(t: UserLogin, context: IterativeCondition.Context[UserLogin]): Boolean = {
          t.status == "fail"
        }
      })

    //3、使用模式提取数据
    val patternStream = CEP.pattern(dataStream, pattern1)

    //4、处理匹配到的流数据
    patternStream.process(new PatternProcessFunction[UserLogin,String] {
      override def processMatch(map: util.Map[String, util.List[UserLogin]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
        `map`.get("begin")  //这里的"begin"  就是定义模式时间给的名称，不过，为什么要加 ` ？
//        map.get("begin")

      }
    })


  }
}
