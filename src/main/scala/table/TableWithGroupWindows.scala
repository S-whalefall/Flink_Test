package table

import bean.TrainAlarm
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{$, AnyWithOperations, LiteralIntExpression, Session, Slide, Tumble}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.time.Duration

object TableWithGroupWindows {
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

    //1、在TableAPI中转换为Table的时候指定
    val tableTemp = tenv.fromDataStream(tableStream, $("id"), $("ts").rowtime().as("rt"), $("temp"))
      .window(Tumble.over(10.seconds()).on("rt").as("w"))
//      .window(Slide.over(10.seconds()).every(2.seconds()).on("rt").as("w"))
//      .window(Session.withGap(10.seconds()).on("rt").as("w"))
      .groupBy($("id"),$("w"))
      .select($("id").count(),$("w").end(),$("temp").max())    //$("w").end(), 窗口的结束时间

    tenv.toAppendStream[Row](tableTemp).print()


    //2、在创建表的时候指定
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
        | id,
        | count(id) cnts,
        | tumble_end(rt,interval '10' second) win_end,
        | max(temp)
        ||from t_person
        |group by id,tumble(rt,interval '10' second)
        |""".stripMargin)


    env.execute()
  }
}
