package wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //创建环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据Source,host是配置的ip，port也是
//    val inputStream = env.socketTextStream("host", port = 1314)  //运行命令nc -lk 1314
    val inputStream = env.readTextFile("D:\\projects\\Flink_Test\\src\\source1")

    //转换操作
    val result = inputStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1) //基于key分组
      .sum(1) //对位置为1的进行求和，从0开始，1就是第二位

    //输出Sink
    result.print()

    //执行
    env.execute()
  }
}
