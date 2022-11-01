package table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.{$, FieldExpression, call}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row



object TableFuncDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tenv = StreamTableEnvironment.create(env)

    val dataStream = env.fromElements("aa_a", "x_xx", "c_as_da")

/*
* 调用的时候，都用了call
*
* */

    //1、直接在TableAPI中调用
    val result = tenv.fromDataStream(dataStream, $("line"))
      .joinLateral(call(classOf[MyTableFunc], $"line"))
      .select($"line", $"word", $"length")

    tenv.toAppendStream[Row](result).print()


    //2、先注册，再调用
    tenv.createFunction("splitTion",classOf[MyTableFunc])
    tenv.fromDataStream(dataStream, $("line"))
      .joinLateral(call("splitTion", $"line"))
      .select($"line", $"word", $"length")

    //3、从sql里使用，
    tenv.sqlQuery(
      "SELECT myField, word, length " +
        "FROM MyTable, LATERAL TABLE(MyTableFunc(myField))");
    env.execute()
  }
}

@FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
class MyTableFunc extends TableFunction[Row]{

  //多个值输出：使用collect方法进行输出
  def eval(str:String): Unit ={
      str.split("_").foreach(s=>collect(Row.of(s,Int.box(s.length))))

  }


}