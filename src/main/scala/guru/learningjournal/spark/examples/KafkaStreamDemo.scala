package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, from_json, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import com.databricks.spark.xml._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object KafkaStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Kafka Stream XML Processing")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true)))

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "xmldata")
      .option("startingOffsets", "latest")
      .option("rowTag", "book")
      .load()

    kafkaDF.printSchema()
//      val valueDF = kafkaDF
//        .selectExpr("CAST(value AS STRING) as payload")
//   val valueDF = kafkaDF.select("author", "_id")
 //   val valueDF =  kafkaDF.withColumn("xml_data", expr("CAST(value as string)") )

    val kafkaValueAsStringDF = kafkaDF.selectExpr("CAST(key AS STRING) msgKey","CAST(value AS STRING) xmlString")
    val valueDF = kafkaValueAsStringDF.selectExpr(
        "xpath(xmlString, '/catalog/book/author/text()') as authorAsString")
      valueDF.printSchema()
  //   valueDF.show()
//    val values = kafkaDF.select(expr("CAST(value as string)") )
//    val extractValuesFromXML = udf { (xml: String) => ??? }
//     val numbersFromXML = values.withColumn("xml_data", extractValuesFromXML(values))
//    //valueDF.printSchema()

//        .selectExpr("xpath(xml_data, '/users/user/user_id/text()') as user_id")
//      "xpath_long(payload, '/CofiResults/ExecutionTime/text()') as ExecutionTimeAsLong",
//      "xpath_string(payload, '/CofiResults/ExecutionTime/text()') as ExecutionTimeAsString",
//      "xpath_int(payload, '/CofiResults/InputData/ns2/HeaderSegment/Version/text()') as VersionAsInt")
//    val valueDF = kafkaDF.select(from_json(col("value").cast("string"), schema).alias("value"))
  //   valueDF.printSchema()
//    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
//      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
//      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")
//
//    val flattenedDF = explodeDF
//      .withColumn("ItemCode", expr("LineItem.ItemCode"))
//      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
//      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
//      .withColumn("ItemQty", expr("LineItem.ItemQty"))
//      .withColumn("TotalValue", expr("LineItem.TotalValue"))
//      .drop("LineItem")
//
//    val invoiceWriterQuery = flattenedDF.writeStream
//      .format("json")
//      .queryName("Flattened Invoice Writer")
//      .outputMode("append")
//      .option("path", "output")
//      .option("checkpointLocation", "chk-point-dir")
//      .trigger(Trigger.ProcessingTime("1 minute"))
//      .start()
       val query = valueDF.writeStream
          .outputMode("append")
          .option("path", "output")
          .format("console")
          .format("json")
          .option("checkpointLocation", "chk-point-dir")
          .trigger(Trigger.ProcessingTime("1 minute"))
          .start()
          logger.info("Listening to Kafka")
          query.awaitTermination()
//    val query = valueDF
//      .writeStream
//      .queryName("Notification Writer")
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic", "xmldata-out")
//      .outputMode("append")
//      .option("checkpointLocation", "chk-point-dir")
//      .trigger(Trigger.ProcessingTime("1 minute"))
//      .start()
//      logger.info("Listening to Kafka")
//      query.awaitTermination()

  }

}
