package broadcast_join

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nameAndClass = env.socketTextStream("master", 666)
      .map(line => {
        val splits = line.split(" ")
        (splits(0), splits(1))
      })

    val nameAndAge = env.addSource(new MySourceFunc2)

    nameAndClass.join(nameAndAge)  //将两个里面的字段拿出来进行比对连接
      .where(_._1)   //nameAndClass的第一个字段
      .equalTo(_._1) //nameAndAge 的第一个字段
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply(
        {
          (t1,t2) => (t1._1 + "," + t1._2 + "," + t2._2)
        }
      ).print()

    env.execute()

  }
}


class MySourceFunc2 extends SourceFunction[(String,Int)]{
  override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
    while (true){
      Thread.sleep(500)
      //0-9随机数
      //Student(sid:Long,name:String,age:Int,score:Int)
      val random = new Random().nextInt(10)
      if (random >= 7){
        ctx.collect(("tom",18))
      }else{
        ctx.collect(("jim",20))
      }
    }

  }

  override def cancel(): Unit = ???
}


