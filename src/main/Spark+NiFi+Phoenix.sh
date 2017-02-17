import org.apache.nifi._
import java.nio.charset._
import org.apache.nifi.spark._
import org.apache.nifi.remote.client._
import org.apache.spark._
import org.apache.nifi.events._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.nifi.remote._
import org.apache.nifi.remote.client._
import org.apache.nifi.remote.protocol._
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import java.io._
import org.apache.spark.serializer._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkNiFiAttribute extends App {

val conf = new SiteToSiteClient.Builder().url("http://localhost:9090/nifi").portName("spark").buildConfig()
val config = new SparkConf().setAppName("Nifi_Spark_Data").setMaster("local[4]")
val sc = new SparkContext(config)
val ssc = new StreamingContext(sc, Seconds(10))
val lines = ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY))

lines.foreachRDD {rdd =>
 val count = rdd.map{dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8)}.count().toInt
 
 val content_rdd = rdd.map{dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8)}.collect()
 val uuid_rdd = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("uuid"))}.collect()
 val bulletin_level_rdd = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("BULLETIN_LEVEL"))}.collect()
 val event_date_rdd = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("EVENT_DATE"))}.collect()
 val event_type_rdd = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("EVENT_TYPE"))}.collect()

 for (i <- 0.until(count)) {
      val content: String = content_rdd(i)
      val uuid: String = uuid_rdd(i)
      val bulletin_level: String = bulletin_level_rdd(i)
      val event_date: String = event_date_rdd(i)
      val event_type: String = event_type_rdd(i)
      sc.parallelize(Seq((uuid, event_date, bulletin_level, event_type, content))).saveToPhoenix("NIFI_SPARK",Seq("UUID","EVENT_DATE","BULLETIN_LEVEL","EVENT_TYPE","CONTENT"),zkUrl = Some("localhost:2181:/hbase-unsecure"))
    }
 }
ssc.start()
ssc.awaitTermination()
}
SparkNiFiAttribute.main(Array())
