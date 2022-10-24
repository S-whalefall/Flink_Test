package state

import bean.TrainAlarm
import org.apache.flink.api.common.functions.{RichMapFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object StateDeclareDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val result = env.socketTextStream("master", 666)
      .map(line => {
        val splits = line.split(",")
        TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
      })
      .map(new RichMapFunctionWithState)
  }
}


class RichMapFunctionWithState extends RichMapFunction[TrainAlarm,String]{
  //一般定义状态的时候，把状态定义到open中，或者使用懒加载的方式定义状态

  var tempstate: ValueState[Double] = _

  lazy val listState = getRuntimeContext.getListState(new ListStateDescriptor[Double]("liststate",classOf[Double]))
  lazy val MapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("Mapstate",classOf[String],classOf[Double]))

  lazy val reduceState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Double]("reduceState",new RichReduceFunction[Double] {
    override def reduce(value1: Double, value2: Double): Double = value1 + value2
  },classOf[Double]))

  override def open(parameters: Configuration): Unit = {
    //定义一个值状态
    tempstate = getRuntimeContext.getState(new ValueStateDescriptor[Double]("tempstate", classOf[Double]))

  }

  override def map(value: TrainAlarm): Unit = {
    //获取
    tempstate.value()
    tempstate.update(value.temp)

    listState.add(value.temp)
    listState.get().iterator()

    MapState
    tempstate.value()


  }
}