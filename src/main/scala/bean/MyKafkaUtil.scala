package bean


import java.util.Properties

import MyKafkaUtil.kafkaParams
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {
  private val properties: Properties = MyProperties.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")
  var kafkaParams = collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,//用于初始化链接到集群的地址
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    ConsumerConfig.GROUP_ID_CONFIG ->"gmall0621_group",
    //latest自动重置偏移量为最新的偏移量
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )

  def getKafkaSteam(topic:String,ssc:StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))
    inputDStream

  }

  def getKafkaSteam(topic:String,ssc:StreamingContext,groupID:String):InputDStream[ConsumerRecord[String, String]] = {
    kafkaParams(ConsumerConfig.GROUP_ID_CONFIG)=groupID
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))
    inputDStream

  }
  def getKafkaSteam(topic:String,ssc:StreamingContext,groupID:String,offests:Map[TopicPartition,Long]):InputDStream[ConsumerRecord[String, String]] = {
    kafkaParams(ConsumerConfig.GROUP_ID_CONFIG)=groupID
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams,offests))
    inputDStream

  }


}
