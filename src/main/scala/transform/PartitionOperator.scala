package transform

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._


/*
* 进行分区操作，这里是自定义分区
* */
object PartitionOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    env.fromElements("aa","bb","cc","aa")
      .map((_,1))
      .partitionCustom(new MyPartitioner,_._1)
      .print()

    env.execute()


  }
}

class MyPartitioner extends Partitioner[String]{
  override def partition(k: String, i: Int): Int = {
    if (k.equals("aa")){
      0
    }else{
      1
    }
  }
}
