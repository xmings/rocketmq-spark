package org.apache.rocketmq.spark.sql

import com.google.common.io.Files
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.jupiter.api.Test
import org.slf4j.event.Level
import org.slf4j.{Logger, LoggerFactory}

class MyRocketMQDataSourceTest {
  private val logger = LoggerFactory.getLogger(classOf[MyRocketMQDataSourceTest])

  private val SPARK_CHECKPOINT_DIR = Files.createTempDir.getPath // ensure no checkpoint

  @Test
  def testStreaming(): Unit = {
    val spark = SparkSession.builder
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR)
      .master("local[4]")
      .appName("RocketMQSourceProviderTest")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.readStream
      .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
      .option("nameServer", "47.97.65.248:9876")
      .option("topic", "AFANTI_DONGCHEDI_DATA_DEV")
//      .option("group", "GID_SPARK_STREAM_TEST")
      .load()

    val query = data
      .writeStream
      .foreachBatch((df: Dataset[Row], id: Long) =>{
        df.show()
      })
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    query.awaitTermination()

  }
}
