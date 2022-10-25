package table

//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object TableAndSQLDemo1 {
  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()

    val tEnv = TableEnvironment.create(settings)


  }
}
