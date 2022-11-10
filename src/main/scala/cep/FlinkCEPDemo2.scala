package cep

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

object FlinkCEPDemo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)
    )).assignAscendingTimestamps(_.ts * 1000)
      .keyBy(_.orderId)//因为时间是单调递增，用ts字段作为水位线 ,  设置为EventTime

    val pattern1 = Pattern.begin[OrderEvent]("begin")
      .where(_.behavior == "create")
      .followedBy("follew")
      .where(_.behavior == "pay")
      .within(Time.minutes(15))  //定义必须完成匹配模式才能被视为有效的最大时间间隔，说明此次pattern和上一个比较，时间才15min之内

    val patternStream = CEP.pattern(orderEventStream, pattern1)

    //对pattern产生的数据流进行处理
    //这里还考虑了侧输入流的问题，考虑一些迟到的数据的处理

    val lateTag = new OutputTag[OrderResult]("lateTag")

    val result = patternStream.select[OrderResult, OrderResult](lateTag,
      //处理超时数据
      new PatternTimeoutFunction[OrderEvent, OrderResult] {
        override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
          val orderEvent = map.get("begin").iterator().next() //迭代读取所有数据   转换成迭代器，获取里面的值
          OrderResult(orderEvent.orderId, orderEvent.behavior, "time out")
        }
      },
      //处理正常数据
      new PatternSelectFunction[OrderEvent, OrderResult] {
        override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
          val orderEvent = map.get("begin").iterator().next()
          OrderResult(orderEvent.orderId, orderEvent.behavior, "normal")
        }
      }
    )

    result.print()
    result.getSideOutput(lateTag).print()


    env.execute()
  }
}

case class OrderResult(orderId:Int,behavior:String,info:String)

case class OrderEvent(orderId:Int,behavior:String,ts:Long)