import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import bean.{JedisClient, MyKafkaUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("577's Dau").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ODS_DAU0820"
    val groupID="577"

    var kafkainputstream: InputDStream[ConsumerRecord[String, String]] = null

    kafkainputstream=MyKafkaUtil.getKafkaSteam(topic, ssc, groupID)

    val kafkavalues: DStream[String] = kafkainputstream.map(_.value())
    val result: DStream[JSONObject] = kafkavalues.map(value => {
      val jsobj: JSONObject = JSON.parseObject(value)
      val time: lang.Long = jsobj.getLong("ts")
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      val realtime: String = sdf.format(new Date(time))
      val Dayhour: Array[String] = realtime.split(" ")
      val day: String = Dayhour(0)
      val hour: String = Dayhour(1)
      jsobj.put("dt", day)
      jsobj.put("hour", hour)
      jsobj
    })
  //  result.print(1000)
//    result.filter(value=>{
//      val day: String = value.getString("dt")
//      val mid: String = value.getJSONObject("common").getString("mid")
//         val key ="dau:"+day
//       null
//     })
    result.mapPartitions(jsobj=>{
      val jedisclinet: Jedis = JedisClient.getJedisclinet
      val ExistsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
      for (js <- jsobj) {
      val day: String = js.getString("dt")
        val mid: String = js.getJSONObject("common").getString("mid")
        val daykey="dau:"+day
        val EXISTS: lang.Long = jedisclinet.sadd(daykey, mid)
        if (jedisclinet.ttl(daykey) < 0) {
          jedisclinet.expire(daykey, 3600 * 24)         //什么操作这是？客户端里面的属性？不是应该objectS里面的吗
        }
        if (EXISTS==1L){
          ExistsList.append(js)
        }
      }
      jedisclinet.close()
      ExistsList.toIterator
    })

ssc.start()
    ssc.awaitTermination()

  }



}
