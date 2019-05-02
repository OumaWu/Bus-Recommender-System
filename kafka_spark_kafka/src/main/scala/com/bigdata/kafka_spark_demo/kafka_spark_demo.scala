package com.bigdata.kafka_spark_demo


import java.util.Properties

import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object createKafkaProducerPool{

  def apply(brokerList: String, topic: String):  GenericObjectPool[KafkaProducerProxy] = {
    val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
  }
}

object kafka_spark_demo {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建topic
    val brobrokers = "master:9092,slave1:9092,slave2:9092"
    val sourcetopic="source";
    val targettopic="target";

    //创建消费者组
    var group="con-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );


    //ssc.sparkContext.broadcast(pool)

    //创建DStream，返回接收到的输入数据
    val stream=KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))


    val buskeeper = stream.map{case msg=>
     var attr = msg.value().split("-")
      (attr(0),attr(1).toInt)}

    buskeeper.foreachRDD{ rdd =>
      rdd.map{s=>
        println("UserName: "+s._1+"  Music_id: "+s._2)
        val pool = createKafkaProducerPool(brobrokers, targettopic)
        if(s._2 != 0) {
          val p = pool.borrowObject()
          p.send("recommend:  No.1 " + scala.util.Random.nextInt(1000).toString+ "  No.2 " + scala.util.Random.nextInt(1000).toString + "  No.3 " + scala.util.Random.nextInt(1000).toString, Option(targettopic))
        // Returning the producer to the pool also shuts it down
          pool.returnObject(p)}
      }.count()

    }


    ssc.start()
    ssc.awaitTermination()
  }
}
