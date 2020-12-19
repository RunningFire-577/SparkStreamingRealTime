package bean

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisClient {
  private var  pool:JedisPool=null;
  def getJedisclinet:Jedis={
    if(pool==null)
      build()

    pool.getResource


  }
  def build()={
    val properties: Properties = MyProperties.load("config.properties")
    val host: String = properties.getProperty("redis.host")
    val port: String = properties.getProperty("redis.port")
    val config = new JedisPoolConfig
    config.setMaxTotal(100)
    config.setMaxIdle(100)
    config.setMinIdle(20)
    config.setBlockWhenExhausted(true)
    config.setMaxWaitMillis(5000)
    config.setTestOnBorrow(true)
    pool=new JedisPool(config,host,port.toInt)
  }

  def main(args: Array[String]): Unit = {
    val jds: Jedis = JedisClient.getJedisclinet
    println(jds.ping())

}

}
