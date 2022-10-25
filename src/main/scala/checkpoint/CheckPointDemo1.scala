package checkpoint

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckPointDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //FsStateBackend
//    env.setStateBackend(new HashMapStateBackend)   //HashMapStateBackend在flink1.13后有
//    env.getCheckpointConfig.setCheckpointStorage("file:///d:/abc/ckp")
    env.getCheckpointConfig.setCheckpointStorage("hdfs://master:9000/flink/checkpoint")
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L) //checkpoint使用时间超过这个，表示超时失败
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) //同时可以做几个ck
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) //两个ck执行过程中最小间距

    //手动取消任务的时候，是否删除Checkpoint目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  }
}
