package io.keepcoding.data.simulator.batch

import io.keepcoding.data.simulator.streaming.StreamJobImpl.spark
import io.keepcoding.spark.exercise.batch.BatchJob
import org.apache.spark.sql.functions.{col, lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BatchJobImpl extends BatchJob {

  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Batch Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readMobileMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichGroupedData(mobileDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    mobileDF
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app", $"year", $"month", $"day", $"hour")
      .groupBy(window($"timestamp", "1 hour"),$"id")
      .agg(sum($"bytes").as("bytes"))
      .select($"window.start".as("timestamp"), $"id", $"bytes")
      .join(metadataDF,
        mobileDF("id") === metadataDF("id")
      ).drop(metadataDF("id"))
  }

  override def enrichMobileWithMetadata(mobileDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    mobileDF
      .join(metadataDF,
        mobileDF("id") === metadataDF("id")
      ).drop(metadataDF("id"))
  }

  override def computeBytesByAntennaIDAPP(dataFrame: DataFrame, groupByColumn: String, typeName: String): DataFrame = {
    dataFrame
      .select(col(groupByColumn), $"bytes", $"timestamp")
      .groupBy(col(groupByColumn), window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value")
      ).select($"window.start".as("timestamp"), col(groupByColumn).as("id"), $"value", lit(typeName).as("type"))
  }

  override def computeUserQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter($"bytes" >= $"quota")
      .select($"timestamp", $"bytes".as("usage"), $"email", $"quota")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
    }

  def main(args: Array[String]): Unit = run(args)
}

