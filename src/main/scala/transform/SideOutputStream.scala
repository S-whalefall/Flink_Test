package transform

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val oddOutputTag = new OutputTag[Int]("odd")

    val result = env.fromElements(1,2,3,4,5,6)
      .process(new ProcessFunction[Int,Int] {
        override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context, collector: Collector[Int]): Unit = {
          if(i%2==0){
            collector.collect(i)
          }else{
            context.output(oddOutputTag,i) //通过标签的 方式，将数据输出到侧输出流
          }
        }
      })

    result.print("偶数：")
    result.getSideOutput(oddOutputTag).print("奇数：")

    env.execute()


  }
}
