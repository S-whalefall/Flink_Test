package table

import bean.TrainAlarm
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row


/*
* DS和Table的相互转换
* 记清楚org.apache.flink.streaming.api.scala    是DS获取的
* */
object DataStream2Table {
  def main(args: Array[String]): Unit = {


    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(environment)

    val dataStream = environment.readTextFile( "D:\\projects\\Flink\\Flink_Test\\src\\source2")

    val mapped = dataStream.map(line => {
      val strings = line.split(",")
      TrainAlarm(strings(0),strings(1).toLong,strings(2).toDouble)
    })

    //DataStream ->  Table
    //val table = tEnv.fromDataStream(mapped,$("name").as("username"), $("address"))
    val table = tEnv.fromDataStream(mapped,$("id"),$("ts"), $("temp"))

    tEnv.createTemporaryView("student", table)

    val table2 = tEnv.sqlQuery("select id, ts from student where id ='train_1'")

    //Table -> DataStream
    tEnv.toAppendStream[Row](table).print()    //[Row]输出的数据类型
    tEnv.toRetractStream[Row](table2).print()
    //前面多了个Boolean类型的数据，和上面所有数据都以append的形式追加相比，
    // 有true和false对应相关的数据看是否要保留，比如max，那后面来的数据小的就不会被保留，按数据输入顺序结合sqlquery语句进行过滤，或者根据算子进行过滤


    environment.execute()   //要加这条语句，程序才会运行，懒加载机制



  }
}
