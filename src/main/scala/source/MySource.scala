package source

import bean.Student
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

object MySource {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment.setParallelism(2)
//      val env = StreamExecutionEnvironment.getExecutionEnvironment.setParallelism(2) 不行，只能单线程，无法设置为2或以上，继承SourceFunction类

      env.addSource(new MySourceFunc).print()

      env.execute()
  }
}


class MySourceFunc extends SourceFunction[Student]{
  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    while(true){
      /*这种方法要会熟练使用*/
      Thread.sleep(500)
      val random = new Random().nextInt(10)

      sourceContext.collect(Student(random,"XX"+random,random+20,random+90))
    }

  }

  override def cancel(): Unit = ???
}