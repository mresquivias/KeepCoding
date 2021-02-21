package io.keepcoding.data.simulator.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class MobileMessage(timestamp: Timestamp, id: String, antenna_id: String, bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readMobileMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichMobileaWithMetadata(mobileDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesByAntennaIDAPP(dataFrame: DataFrame, groupByColumn: String, typeName: String): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath,
    groupByAntenna, groupByID, groupByApp, typeNameAntenna, typeNameID, typeNameApp) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val mobileDF = parserJsonData(kafkaDF)
    val metadataDF = readMobileMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val deviceMetadataDF = enrichMobileaWithMetadata(mobileDF, metadataDF)
    val storageFuture = writeToStorage(mobileDF, storagePath)
    val aggByAntenna = computeBytesByAntennaIDAPP(deviceMetadataDF, groupByAntenna, typeNameAntenna)
    val aggByID = computeBytesByAntennaIDAPP(deviceMetadataDF, groupByID, typeNameID)
    val aggByApp = computeBytesByAntennaIDAPP(deviceMetadataDF, groupByApp, typeNameApp)
    val aggFutureAntenna = writeToJdbc(aggByAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureID = writeToJdbc(aggByID, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureApp = writeToJdbc(aggByApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)


    Await.result(Future.sequence(Seq(aggFutureAntenna, aggFutureID, aggFutureApp, storageFuture)), Duration.Inf)

    spark.close()
  }

}
