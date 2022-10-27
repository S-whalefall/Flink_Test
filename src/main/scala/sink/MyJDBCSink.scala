package sink

import bean.Worker

import java.sql.Connection
//import com.mysql.jdbc.{Connection, PreparedStatement}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.operators.Driver
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql
import java.sql.DriverManager

object MyJDBCSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Worker] = env.readTextFile("C:\\Users\\14918\\IdeaProjects\\flink-test\\src\\source1")
      .map(line => {  //读文件是一行一行处理的
        val splits = line.split(",")
        Worker(splits(1), splits(2).toInt) //用Worker的意义在哪？是有什么用处？
      })

//    dataStream.addSink()
  }
}
/*
* 问题：总是有mysql和sql的两个jar的冲突
* */

class MyJDBCSinkFunc extends RichSinkFunction[Worker]{

  var conn : Connection = _
  var updateStatement : sql.PreparedStatement = _
  var insertStatement : sql.PreparedStatement = _
//  var updateStatement : PreparedStatement = _
//  var insertStatement : PreparedStatement = _

  override def open(parameters: Configuration): Unit = { //连接mysql
    val conn = DriverManager.getConnection("", "", "")
    updateStatement = conn.prepareStatement("update worker set salary = ? where name =?")
    insertStatement = conn.prepareStatement("insert into worker values (?,?)")
  }

  override def invoke(value: Worker, context: SinkFunction.Context): Unit = {  //这个逻辑自己后面改一下，这个是判断更新的条数为0，然后插入
    updateStatement.setString(1,value.name)
    updateStatement.setInt(2,value.age)
    updateStatement.execute()
    if (updateStatement.getUpdateCount == 0){
      insertStatement.setString(1,value.name)
      insertStatement.setInt(2,value.age)
      insertStatement.execute()
    }
  }

  override def close(): Unit = {
    if (insertStatement != null) insertStatement.close()
    if (updateStatement != null) updateStatement.close()
    if (conn != null) insertStatement.close()
  }
}