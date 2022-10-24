package state

import bean.TrainAlarm
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempRiseAlarmWithState2 {
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
      .process(new TempRiseWithTime)
      .print()

    env.execute()

  }
}

class TempRiseWithTime extends KeyedProcessFunction[String,TrainAlarm,String]{

  //设置了3个状态，温度状态，定时器状态，计数状态
  lazy val tempState = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("temp",classOf[Double]))
  lazy val timerState = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer",classOf[Long]))
  lazy val countState = getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("count",classOf[Int]))


  override def processElement(i: TrainAlarm, context: KeyedProcessFunction[String, TrainAlarm, String]#Context, collector: Collector[String]): Unit = {
    //如果是第一条数据，更新温度状态，注册10s后触发的定时器，并更新时间状态，个数状态设为1
    if(tempState.value() == 0 || timerState == 0 ){
      tempState.update(i.temp)
      context.timerService().registerProcessingTimeTimer(i.ts *1000L + 10000L)
      countState.update(1)
    }else{
      //如果不是第一条数据，更新温度状态值，个数状态值+1
      if(i.temp > tempState.value()){
        tempState.update(i.temp)
        countState.update(countState.value()+1)
      }else{
        //温度低的话，删除定时器（从时间状态中取时间），个数状态置为1， 重新注册定时器并且
        context.timerService().deleteEventTimeTimer(timerState.value())
        countState.update(1)
        context.timerService().registerProcessingTimeTimer(i.ts*1000L + 10000L)
        timerState.update(i.ts * 1000L + 10000L)
        tempState.update(i.temp)
      }
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, TrainAlarm, String]#OnTimerContext, out: Collector[String]): Unit = {
    if(countState.value()>=2){
      out.collect(ctx.getCurrentKey + "is alarming")
      tempState.clear()
      countState.clear()
      timerState.clear()
    }

  }
}
