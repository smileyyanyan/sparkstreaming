package com.sanutty.spark.streaming;

import static com.sanutty.spark.streaming.util.Constants.column_name_AveOccup;
import static com.sanutty.spark.streaming.util.Constants.column_name_features;
import static com.sanutty.spark.streaming.util.Constants.column_name_house_age;
import static com.sanutty.spark.streaming.util.Constants.column_name_lat;
import static com.sanutty.spark.streaming.util.Constants.column_name_lon;
import static com.sanutty.spark.streaming.util.Constants.column_name_med_inc;
import static com.sanutty.spark.streaming.util.Constants.column_name_number_of_bedrooms;
import static com.sanutty.spark.streaming.util.Constants.column_name_number_of_rooms;
import static com.sanutty.spark.streaming.util.Constants.column_name_population;
import static com.sanutty.spark.streaming.util.Constants.column_name_price;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sanutty.spark.streaming.entities.HouseInfo;
import com.sanutty.spark.streaming.functions.MapCSVMessageToHouseInfoFunction;
import com.sanutty.spark.streaming.functions.MapKafkaMessageToHouseInfoFunction;

public class HousePricePredictorSparkStreamingWithKafka {

    private static final String csvPath = "src/main/resources/historical_housing_with_price.csv";
    private static final String KAFKA_INPUT_TOPIC_HOUSING = "input_topic_housing";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    
    public static void main( String[] args )  {
    
      try {	
	    //-------------------------------------------
	    // The commented section below uses the deprecated JavaStreamingContext.
	    // It has been replaced with Spark Structured Streaming
	    // Code left here for reference. 
	    //-------------------------------------------    
	    
	    SparkSession spark = SparkSession
	                        .builder()
	                        .appName("House Price Predictor")
	                        .master( "local[*]" )
	                        .getOrCreate();
	
        //-------------------------------------------
        // define an explicit schema for the csv to be read
        //-------------------------------------------
        StructField defMedInc= new StructField(column_name_med_inc, DataTypes.DoubleType, false, Metadata.empty());
        StructField defHouseAge = new StructField(column_name_house_age, DataTypes.DoubleType, false, Metadata.empty());
        StructField defAveRoom = new StructField(column_name_number_of_rooms, DataTypes.DoubleType, false, Metadata.empty());
        StructField defAveBR = new StructField(column_name_number_of_bedrooms, DataTypes.DoubleType, false, Metadata.empty());
        StructField defPopulation = new StructField(column_name_population, DataTypes.DoubleType, false, Metadata.empty());
        StructField defAveOccup = new StructField(column_name_AveOccup, DataTypes.DoubleType, false, Metadata.empty());
        StructField defLat = new StructField(column_name_lat, DataTypes.DoubleType, false, Metadata.empty());
        StructField defLon = new StructField(column_name_lon, DataTypes.DoubleType, false, Metadata.empty());
        StructField defPrice = new StructField(column_name_price, DataTypes.IntegerType, false, Metadata.empty());
        
        
        StructField[] housingSchemaFields = new StructField[] {
                defMedInc,
                defHouseAge,
                defAveRoom,
                defAveBR,
                defPopulation,
                defAveOccup,
                defLat,
                defLon,
                defPrice
        };
        StructType housingSchema = new StructType(housingSchemaFields);
	        
        //-------------------------------------------
        // Read historical housing info from csv
        //-------------------------------------------
        Dataset<Row> historicalHouseRow = spark.read()
                .option("delimiter", ",")
                .option("header", "true")
                .schema( housingSchema )
                .csv(csvPath);

	    historicalHouseRow.show(1);
	        
        //-------------------------------------------
        // Map Dataset<Row> to  Dataset<HouseInfo>
        //-------------------------------------------	         
	    Encoder<HouseInfo> houseInfoEncoder = Encoders.bean(HouseInfo.class);
	    Dataset<HouseInfo> historicalHouseInfoDS = historicalHouseRow.map(new MapCSVMessageToHouseInfoFunction(), houseInfoEncoder);
	    historicalHouseInfoDS.show(2);

        //-------------------------------------------	    
	    //Define features fields
        //-------------------------------------------	    
	    VectorAssembler vectorAssembler = new VectorAssembler()
            .setInputCols(new String[]{ column_name_med_inc, 
            							column_name_house_age, 
            							column_name_number_of_rooms, 
            							column_name_number_of_bedrooms, 
            							column_name_population, 
            							column_name_AveOccup, 
            							column_name_lat, 
            							column_name_lon
            							})
            .setOutputCol(column_name_features);
      
	        
	      //-------------------------------------------
	      // Use RandomForrest Regressor to fit the data
	      //-------------------------------------------
	      RandomForestRegressor randomForestRegressor = new RandomForestRegressor()
									      .setLabelCol(column_name_price)
									      .setFeaturesCol(column_name_features)
									      .setNumTrees(10); 
	      
	      //-------------------------------------------
	      // Create a Pipeline to chain the feature assembler and the model
	      //-------------------------------------------
	      Pipeline pipeline = new Pipeline() 
	                .setStages(new PipelineStage[]{vectorAssembler, randomForestRegressor});
	      
	      //-------------------------------------------
	      // 5. Train the model
	      //-------------------------------------------	      
	      PipelineModel model = pipeline.fit(historicalHouseInfoDS);
	      
        //-------------------------------------------
        // Read data from Kafka structured streaming
        //-------------------------------------------

         Dataset<Row> kafkaStreamDS = spark.readStream()
                        					.format("kafka")
						                    .option( "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS )
						                    .option("subscribe", KAFKA_INPUT_TOPIC_HOUSING)
						                    .option("startingOffsets", "latest")
						                    .load();

         Dataset<Row> houseDSAsString = kafkaStreamDS.selectExpr("CAST(value AS STRING) AS value");

         //-------------------------------------------
         // Map Dataset<Row> to  Dataset<HouseInfo>
         //-------------------------------------------	         
         Dataset<HouseInfo> testdataDS= houseDSAsString.map(new MapKafkaMessageToHouseInfoFunction(), houseInfoEncoder);

        //-------------------------------------------	         
        // Train the model
	    //-------------------------------------------	         
	    System.out.println("Training the model...");

       // 7. Make predictions on the test data
       Dataset<Row> predictions = model.transform(testdataDS);
         
       // 8. Show the predictions
       System.out.println("Predictions:");
         
       predictions.writeStream()
  		.format("console")
  		.start()
  		.awaitTermination();
	         
      } catch (Exception e) {
    	  e.printStackTrace();
      }
        
    }  //main

}
