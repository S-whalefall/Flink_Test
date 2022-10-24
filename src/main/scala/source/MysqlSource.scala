package source

import bean.Worker
import com.mysql.jdbc.PreparedStatement
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

import java.sql
import java.sql.{Connection, DriverManager}

object MysqlSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new MysqlSourceFunc).print()

    env.execute()
  }
}


class MysqlSourceFunc extends RichParallelSourceFunction[Worker]{ //原来只有run和cancel，open和close是RichParallelSourceFunction里其他的两个方法

  var conn :Connection = _ //_ 是占位符的意思
  var statement : sql.PreparedStatement = _  //为什么Java.sql的，不是mysql的？
  var flag = true  //只是为了显示cancel方法的应用，具体逻辑自定

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_librarySys", "root", "1234")
    statement = conn.prepareStatement("SELECT * FROM db_librarySys")
  }

  override def run(sourceContext: SourceFunction.SourceContext[Worker]): Unit = { //这个地方包括上个类返回的泛型，都要自己添加
    while (flag){
      Thread.sleep(5000)
      val resultSet = statement.executeQuery()  //获取的是一个集合
      while (resultSet.next()){  //判断集合还有没有数
        val name = resultSet.getString(1)  //获取字段的值
        val age = resultSet.getInt(2)
        sourceContext.collect(Worker(name,age))  //输出字段值
      }

    }
  }

  override def cancel(): Unit = {
    flag=false
  }

  override def close(): Unit = {
    conn.close()
  }
}