package transform

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object MulitDataStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input1 = env.fromElements(1,2,3,4,5,6)
    val input2 = env.fromElements(2,3,4)
    val unionDataStream: DataStream[Int] = input1.union(input2)


    val input3 = env.fromElements("a", "b", "c")
    val connStream: ConnectedStreams[Int, String] = input1.connect(input3)

    connStream.map(new CoMapFunction[Int,String,String] {
      override def map1(in1: Int): String = {
        "String"+in1
      }

      override def map2(in2: String): String = {
        in2.toUpperCase()

      }
    }).print()

    env.execute()


  }
}
