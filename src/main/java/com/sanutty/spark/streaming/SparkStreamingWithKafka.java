package com.sanutty.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingWithKafka {
  
  private static final String project_id_column_name = "projectid";
  private static final String project_name_column_name = "projectName";
  
  private static final String status_indicator_column_name = "projectStatus";
  private static final String estimated_labor_days_column_name = "estimatedLaborInDays";
  private static final String estimated_labor_cost_dollars_column_name = "estimatedLaborCostInDollars";
  
  private static final String resource_count_column_name = "resourceCount";
  private static final String task_count_column_name = "taskCount";

  public static void main( String[] args ) throws Exception {
    
    //-------------------------------------------
    // The commented section below uses the deprecated JavaStreamingContext.
    // It has been replaced with Spark Structured Streaming
    // Code left here for reference. 
    //-------------------------------------------    
    
    //    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
    //    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
    //    
    //    Map<String, Object> kafkaParams = new HashMap<>();
    //    kafkaParams.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka brokers
    //    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //    kafkaParams.put("group.id", "spark-streaming-group"); // Set a group ID for your consumer
    //    kafkaParams.put("auto.offset.reset", "earliest"); // Or "latest", depending on your needs
    //    kafkaParams.put("enable.auto.commit", false); // Important for managing offsets manually
    //    
    //    Set<String> topicsSet = new HashSet<>(Arrays.asList("input_data_topic")); // Replace with your topic name(s)
    //
    //    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
    //        javaStreamingContext,
    //        LocationStrategies.PreferConsistent(),
    //        ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
    //    );
    //    
    //    messages.foreachRDD( rdd -> {
    //      rdd.foreach(record -> {
    //        System.out.println("Received message: " + record.value());
    //      });
    //    } );
    //    
    //    javaStreamingContext.start();
    //    javaStreamingContext.awaitTermination();
    
    SparkSession spark = SparkSession
                        .builder()
                        .appName("Streaming Project Outliers")
                        .master( "local[*]" )
                        .getOrCreate();
    
    //-------------------------------------------
    // Read data from Kafka using spark.readStream()
    //-------------------------------------------
    
     Dataset<Row> kafkaStream = spark.readStream()
                    .format("kafka")
                    .option( "kafka.bootstrap.servers", "localhost:9092" )
                    .option("subscribe", "input_data_topic")
                    .option("startingOffsets", "latest")
                    .load();

     //-------------------------------------------     
     // Select the key and value columns and cast them to String
     //-------------------------------------------     
     Dataset<Row> processedStream = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");   

     processedStream.writeStream()
                     .format("kafka") // Output to the console
                     .option("kafka.bootstrap.servers", "localhost:9092")
                     .option("topic", "output_data_topic")
                     .option("checkpointLocation", "/tmp/sparkstreaming/checkpoint")
                     .start() 
                     .awaitTermination();
     
    }  //main

}
