package table

import org.apache.flink.table.api._
import org.apache.flink.table.api.Expressions._


object TableAndSQLDemo2 {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()

    val tEnv = TableEnvironment.create(settings)

    //创建输入数据对接kafka，（都是用connect连接的）
    tEnv.executeSql(
      """
        |create table t_name_from(
        | field1 type,
        | field2 type,
        | field3 type,
        | field4 type
        | )with(
        |  'connector' = 'kafka',
        |  'topic' = 'user_behavior',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        | )
        |""".stripMargin)


    //输出数据对接kafka（）要有主键，避免插入重复？
    tEnv.executeSql(
      """
        |create table t_name_to (
        | field1 type,
        | field2 type,
        | field3 type,
        | field4 type
        | PRIMARY KEY(filed1) NOT ENFORCED
        | )with(
        |  'connector' = 'kafka',
        |  'topic' = 'user_behavior',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'key.format' = 'json'
        |  'value.format' = 'json'
        | )
        |""".stripMargin)
    //        |  'properties.group.id' = 'testGroup',
    //        |  'scan.startup.mode' = 'earliest-offset',
    //        |  'format' = 'csv'



  }
}
