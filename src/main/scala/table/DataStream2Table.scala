package table

import bean.TrainAlarm
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/*
* DS和Table的相互转换
* 记清楚org.apache.flink.streaming.api.scala    是DS获取的
* */
object DataStream2Table {
  def main(args: Array[String]): Unit = {


    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(environment)

    val dataStream = environment.readTextFile( "D:\\projects\\Flink\\Flink_Test\\src\\source2.txt")

    val mapped = dataStream.map(line => {
      val strings = line.split(",")
      TrainAlarm(strings(0),strings(1).toLong,strings(2).toDouble)
    })

    //DataStream ->  Table
    val table = tEnv.fromDataStream(mapped,"asd,asdsa,asdasda")

    tEnv.createTemporaryView("student", table)

//    val result = tEnv.sqlQuery("select name, score from student where score > 60")

    //Table -> DataStream
    tEnv.toAppendStream[TrainAlarm](table).print()

//    result.execute().print()

  }
}
