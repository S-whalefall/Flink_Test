package state

import bean.TrainAlarm
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempRiseAlarmWithState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.socketTextStream("master", 666)
      .map(line => {
        val splits = line.split(",")
        TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
      }).assignAscendingTimestamps(_.ts * 1000L)
      /*数据流的时间戳是单调递增的，也就是说没有乱序，那我们可以使用assignAscendingTimestamps，这个方法会直接使用数据的时间戳生成watermark。*/
      .keyBy(_.id)
      .process(new TempRiseAlarmFunc)
      .print()

    env.execute()
  }
}

class TempRiseAlarmFunc extends KeyedProcessFunction[String,TrainAlarm,String]{

  lazy val tempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("tempstate",classOf[Double]))

  override def processElement(i: TrainAlarm, context: KeyedProcessFunction[String, TrainAlarm, String]#Context, collector: Collector[String]): Unit = {
    //判断是不是第一条数据
    if(tempState.value() == 0 ){
      tempState.update(i.temp)
    }else{
      //判断温度差值是否大于等于10
      if((tempState.value() - i.temp).abs >=10){
        collector.collect(context.getCurrentKey +"is alarming")
        tempState.update(i.temp)
      }else{
        //只更新状态
        tempState.update(i.temp)
      }
    }
  }
}