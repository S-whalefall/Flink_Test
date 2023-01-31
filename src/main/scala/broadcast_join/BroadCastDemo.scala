package broadcast_join


import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{BroadcastStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql
import java.sql.{DriverManager, PreparedStatement}



object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //定义为 KV ，一方面是为了广播的时候定义为map，另一方面是为了做关联操作
    val userBaseDS: DataStream[(Long, BaseUserInfo)] = env.addSource(new UserSourceFunc)    //从数据库获取到流
      .map(user => (user.id, user))

    val mapStateDes = new MapStateDescriptor[Long, BaseUserInfo]("mapState", classOf[Long], classOf[BaseUserInfo])   //定义一个map状态描述器

    val broadCastStream: BroadcastStream[(Long, BaseUserInfo)] = userBaseDS.broadcast(mapStateDes)   //将数据库里的信息广播到状态描述器里


    //下面是获取正常的流数据
    val dataInfoDS: DataStream[String] = env.socketTextStream("master", 666)

    //两个流进行connect操作
    var value: DataStreamSink[UserVisitInfo] = dataInfoDS.connect(broadCastStream)
      .process(new MyBroadcastFunc) //这个process里面有两个流分别的处理
      .print()

    env.execute()

  }
}

//定义样例类，接收mysql的用户数据
case class BaseUserInfo(id:Long,name:String,age:Int,city:String,phone:Long )

//输出数据类型
case class UserVisitInfo(id:Long,name:String,city:String,behavior: String,phone:Long )

//实现广播ProcessFunction   String是端口获取的数据流，（L，B）是数据库的状态数据， UserVisitInfo是processFunc运行完后输出的数据类型
class MyBroadcastFunc extends BroadcastProcessFunction[String,(Long,BaseUserInfo),UserVisitInfo]{
  /*
  * 处理逻辑，将广播流中的每个值，写到状态里，然后将这个状态给日志流的数去调用
  * */

  val mapStateDes = new MapStateDescriptor[Long, BaseUserInfo]("mapState", classOf[Long], classOf[BaseUserInfo])   //这里定义的状态和上面主方法里定义的是一样的

  //处理的是日志流中的每条数据
  override def processElement(value: String, ctx: BroadcastProcessFunction[String, (Long, BaseUserInfo), UserVisitInfo]#ReadOnlyContext, out: Collector[UserVisitInfo]): Unit = {
    val use_id= JSON.parseObject(value).getLong("user_id")        //是Json里的字段，用kv的方式获取对应的值
    val behavior = JSON.parseObject(value).getString("behavior")
    val url= JSON.parseObject(value).getLong("url")

    val mapState: ReadOnlyBroadcastState[Long, BaseUserInfo] = ctx.getBroadcastState(mapStateDes)  //将输入的ctx，广播到状态中
    val userInfo = mapState.get(use_id)  //获取用户对应在数据库中的信息

    out.collect(UserVisitInfo(use_id,userInfo.name,userInfo.city,behavior,url))   //把需要的字段用collect进行拼接收集



  }

  //处理的广播流中的每个值
  override def processBroadcastElement(value: (Long, BaseUserInfo), ctx: BroadcastProcessFunction[String, (Long, BaseUserInfo), UserVisitInfo]#Context, out: Collector[UserVisitInfo]): Unit = {


  }
}

//实现获取数据库的用户信息
class UserSourceFunc extends RichParallelSourceFunction[BaseUserInfo]{

  var conn:sql.Connection = _
  var statement :PreparedStatement = _
  var flag:Boolean = true


  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("","","")
    statement = conn.prepareStatement("xxxx")
  }

  override def run(sourceContext: SourceFunction.SourceContext[BaseUserInfo]): Unit = {
    while (flag){
      Thread.sleep(5000)
      val resultSet = statement.executeQuery()   //获取上下文，从状态通道里获取，状态通道里有sql语句执行的
      while (resultSet.next()){
        val id = resultSet.getLong(1)
        val name = resultSet.getString(2)
        val age = resultSet.getInt(3)
        val city = resultSet.getString(4)
        val phone = resultSet.getLong(5)
        sourceContext.collect(BaseUserInfo(id , name , age , city , phone))
      }
    }

  }

  override def cancel(): Unit = ???
}

