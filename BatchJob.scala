package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readMobileMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichGroupedData(mobileDF: DataFrame, metadataDF: DataFrame): DataFrame

  def enrichMobileWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesByAntennaIDAPP(dataFrame: DataFrame, groupByColumn: String, typeName: String): DataFrame

  def computeUserQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, jdbcHourlyTable,jdcbjdbcQuotaTable,
    jdbcUser, jdbcPassword, groupByAntenna, groupByID, groupByApp, typeNameAntenna, typeNameID, typeNameApp) = args
    println(s"Running with: ${args.toSeq}")

    val mobileDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readMobileMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val groupedMobileMetadataDF = enrichGroupedData(mobileDF, metadataDF).cache()
    val mobileMetadataDF = enrichMobileWithMetadata(mobileDF, metadataDF).cache()
    val aggUserQuota = computeUserQuota(groupedMobileMetadataDF)
    val aggAntennaData = computeBytesByAntennaIDAPP(mobileMetadataDF, groupByAntenna, typeNameAntenna)
    val aggUserData = computeBytesByAntennaIDAPP(mobileMetadataDF, groupByID, typeNameID)
    val aggAppData = computeBytesByAntennaIDAPP(mobileMetadataDF, groupByApp, typeNameApp)

    writeToJdbc(aggUserQuota, jdbcUri, jdcbjdbcQuotaTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggAntennaData, jdbcUri, jdbcHourlyTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggUserData, jdbcUri, jdbcHourlyTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggAppData, jdbcUri, jdbcHourlyTable, jdbcUser, jdbcPassword)


    spark.close()
  }
}