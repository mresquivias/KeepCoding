package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, dayofmonth, from_json, hour, lit, max, min, month, sum, window, year}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamJobImpl extends StreamingJob {

  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .option("failOnDataLoss", false)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {

    val schema = ScalaReflection.schemaFor[MobileMessage].dataType.asInstanceOf[StructType]

    dataFrame
      .select(from_json($"value".cast(StringType), schema).as("json"))
      .select("json.*")
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

  override def enrichMobileaWithMetadata(mobileDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    mobileDF
      .join(metadataDF,
        mobileDF("id") === metadataDF("id")
      ).drop(metadataDF("id"))
  }

  override def computeBytesByAntennaIDAPP(dataFrame: DataFrame, groupByColumn: String, typeName: String): DataFrame = {
    dataFrame
      .select($"timestamp", col(groupByColumn), $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minutes"), col(groupByColumn))
      .agg(
        sum($"bytes").as("value")
      ).select($"window.start".as("timestamp"), col(groupByColumn).as("id"), $"value", lit(typeName).as("type"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (df: DataFrame, _: Long) =>
        df
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"$storageRootPath\\data")
      .option("checkpointLocation", s"$storageRootPath\\checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)

}
