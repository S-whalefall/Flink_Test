package table

import bean.TrainAlarm
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
//import org.apache.flink.table.api.{$, AnyWithOperations}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object TableWithProcessTime {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(env)

    val dataStream = env.readTextFile("D:\\projects\\Flink\\Flink_Test\\src\\source2")

    val tableStream = dataStream.map(line => {
      val splits = line.split(",")
      TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    //1、在DataStream转换为Table的时候指定
    val tableTemp = tenv.fromDataStream(tableStream, $("id"), $("ts"), $("temp"), $"pt".proctime())  //AnyWithOperations

    tenv.toAppendStream[Row](tableTemp).print()

    //2、在创建表的时候指定 pt AS PROCTIME()

    val tableTemp2 = tenv.executeSql(
      """
        |create table t_name_from(
        | field1 type,
        | field2 type,
        | field3 type,
        | field4 type,
        | pt AS PROCTIME()
        | )with(
        |   'connector' = 'filesystem'
        |   'path' = 'file:///文件来源路径（记得路径是/）'
        |   'format' = 'csv'
        | )
        |""".stripMargin)



    env.execute()

  }
}


//在DataStream转为为Table的时候指定
