package table

import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api._

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._

object TableAndSQLDemo1 {
  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()

    val tEnv = TableEnvironment.create(settings)

    tEnv.executeSql(
      """
        |create table t_name_from(
        | field1 type,
        | field2 type,
        | field3 type,
        | field4 type
        | )with(
        |   'connector' = 'filesystem'
        |   'path' = 'file:///文件来源路径（记得路径是/）'
        |   'format' = 'csv'
        | )
        |""".stripMargin)
    //

    //创建一张输出表关联外部组件（输出到什么文件）
    tEnv.executeSql(
      """
        |create table t_name_to(
        | field1 type,
        | field2 type,
        | field3 type,
        | field4 type
        | )with(
        |   'connector' = 'filesystem'
        |   'path' = 'file:///文件来源路径（记得路径是/）'
        |   'format' = 'csv'
        | )
        |""".stripMargin)

    tEnv.from("t_name_from").select($("field1"), $("field2"),$("field3")).where("field1 > 1000")

//    tab.filter($"name" === "Fred")
//    tab.where("name = 'Fred'")


  }
}
