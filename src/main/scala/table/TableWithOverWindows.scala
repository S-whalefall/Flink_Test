package table

import bean.TrainAlarm
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{$, AnyWithOperations, FieldExpression, LiteralIntExpression, Over, Tumble}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.time.Duration

object TableWithOverWindows {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(env)

    val dataStream = env.readTextFile("D:\\projects\\Flink\\Flink_Test\\src\\source2")

    val tableStream = dataStream.map(line => {
      val splits = line.split(",")
      TrainAlarm(splits(0), splits(1).toLong, splits(2).toDouble)
    }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[TrainAlarm] {
          override def extractTimestamp(t: TrainAlarm, l: Long): Long = {
            t.ts * 1000L
          }
        })
    )

    //1、在TableAPI中转换为Table的时候指定Over窗口  Over.partitionBy
    val tableTemp = tenv.fromDataStream(tableStream, $("id"), $("ts").rowtime().as("rt"), $("temp"))
      .window(Over.partitionBy("id").orderBy("rt").preceding(2.rows).as("ow"))   //preceding计算时，从当前行再往前包括2行，没有就不算
      .select($("id"),$("temp").avg() over $"ow")    //$("w").end(), 窗口的结束时间

    tenv.toAppendStream[Row](tableTemp).print()


    //2、在创建表的时候指定Over窗口
    val tableTemp2 = tenv.executeSql(
      """
        |create table t_name_from(
        | field1 type,
        | field2 type,
        | ts BIGINT,
        | user_action_time AS TO_TAMPSTAMP(from_unixtime(ts)),
        | WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
        | )with(
        |   'connector' = 'filesystem'
        |   'path' = 'file:///文件来源路径（记得路径是/）'
        |   'format' = 'csv'
        | )
        |""".stripMargin)

    tenv.sqlQuery(
      """
        |select
        | name,
        | avg(salary) over ow,
        | max(salary) over ow
        |from t_person
        |window ow as (
        | partition by name,
        | order by rt
        | rows between 2 preceding and current row
        |)
        |""".stripMargin)


    env.execute()
  }
}
