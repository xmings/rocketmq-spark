package org.apache.rocketmq.spark.sql

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.rocketmq.spark.RocketMQServerMock
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rocketmq.RocketMQSourceProvider
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, IOException}

object RocketMQDataSourceTest {

  private val logger = LoggerFactory.getLogger(classOf[RocketMQDataSourceTest])

  private val CLASS_NAME = classOf[RocketMQSourceProvider].getCanonicalName

  private var mockServer = null

  private var spark = null

  private val SPARK_CHECKPOINT_DIR = Files.createTempDir.getPath // ensure no checkpoint


  @BeforeAll
  @throws[Exception]
  def start(): Unit = {
    val mockServer = new RocketMQServerMock(9878, 10003)
    mockServer.startupServer()
    Thread.sleep(2000)
    val spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR)
      .master("local[4]")
      .appName("RocketMQSourceProviderTest").getOrCreate
  }

  @AfterAll
  def stop(): Unit = {
    mockServer.shutdownServer()
    spark.cloneSession
    try FileUtils.deleteDirectory(new File(SPARK_CHECKPOINT_DIR))
    catch {
      case ex: IOException =>

      /* ignore */
    }
  }
}
