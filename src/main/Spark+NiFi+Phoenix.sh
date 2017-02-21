import org.apache.nifi._
import java.nio.charset._
import org.apache.nifi.spark._
import org.apache.nifi.remote.client._
import org.apache.spark._
import org.apache.nifi.events._
import org.apache.nifi.remote._
import org.apache.nifi.remote.client._
import org.apache.nifi.remote.protocol._
import org.apache.phoenix.spark._
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

object SparkNiFiAttribute extends App {
val conf = new SiteToSiteClient.Builder().url("http://localhost:9090/nifi").portName("spark").buildConfig()
val config = new SparkConf().setAppName("Nifi_Spark_Phoenix").setMaster("local[4]")
val sc = new SparkContext(config)
val ssc = new StreamingContext(sc, Seconds(10))
val lines = ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY))

lines.foreachRDD {rdd =>
 val count = rdd.map{dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8)}.count().toInt
 
 val content = rdd.map{dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8)}.collect()
 val uuid = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("uuid"))}.collect()
 val bulletin_level = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("BULLETIN_LEVEL"))}.collect()
 val event_date = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("EVENT_DATE"))}.collect()
 val event_type = rdd.map{dataPacket => new String(dataPacket.getAttributes.get("EVENT_TYPE"))}.collect()
 
 if(count>0){
 println(s" RDD with count $count received in past 10 Secs =====> Processing")} 
 else{
 println(s" RDD with count $count received in past 10 Secs =====> Nothing To Do")} 
 
 for (i <- 0.until(count)) {
      val content1: String = content(i)
      val uuid1: String = uuid(i)
      val bulletin_level1: String = bulletin_level(i)
      val event_date1: String = event_date(i)
      val event_type1: String = event_type(i)
      sc.parallelize(Seq((uuid1, event_date1, bulletin_level1, event_type1, content1))).saveToPhoenix("NIFI_LOG",Seq("UUID","EVENT_DATE","BULLETIN_LEVEL","EVENT_TYPE","CONTENT"),zkUrl = Some("localhost:2181:/hbase-unsecure"))

    }
 }
ssc.start()
ssc.awaitTermination()
}
SparkNiFiAttribute.main(Array())
