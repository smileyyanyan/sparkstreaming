package com.sanutty.spark.streaming;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
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
import com.sanutty.spark.streaming.functions.MapHouseInfoMapFunction;

public class HousePricePredictorSparkStreamingWithKafka {

    private static final String csvPath = "src/main/resources/historical_housing_with_price.csv";
    private static final String KAFKA_INPUT_TOPIC_HOUSING = "input_topic_housing";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    
    private static final String column_name_med_inc = "MedInc";
    private static final String column_name_house_age = "HouseAge";
    private static final String column_name_ave_rooms = "AveRooms";
    private static final String column_name_ave_bedrms = "AveBedrms";
    private static final String column_name_population = "Population";
    private static final String column_name_AveOccup = "AveOccup";
    private static final String column_name_lat = "Latitude";
    private static final String column_name_lon = "Longitude";
    private static final String column_name_price = "Price";
    private static final String column_name_features = "features";
    
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
	        StructField defAveRoom = new StructField(column_name_ave_rooms, DataTypes.DoubleType, false, Metadata.empty());
	        StructField defAveBR = new StructField(column_name_ave_bedrms, DataTypes.DoubleType, false, Metadata.empty());
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
	      // Read existing housing info from csv
	      //-------------------------------------------
	        Dataset<Row> historicalHousingDataset = spark.read()
	                .option("delimiter", ",")
	                .option("header", "true")
	                .schema( housingSchema )
	                .csv(csvPath);
	
	        System.out.println("=================Total historical data count: " + historicalHousingDataset.count());
	        
	        historicalHousingDataset.show(1);
	        
	      //Define features fields, in this case, features are the same as the input columns   
	      VectorAssembler assembler = new VectorAssembler()
	            .setInputCols(new String[]{ column_name_med_inc, 
	            							column_name_house_age, 
	            							column_name_ave_rooms, 
	            							column_name_ave_bedrms, 
	            							column_name_population, 
	            							column_name_AveOccup, 
	            							column_name_lat, 
	            							column_name_lon, 
	            							column_name_price})
	            .setOutputCol(column_name_features);
	      
	        Dataset<Row> historicalHousingFeatures = assembler.transform(historicalHousingDataset);
	        
	        System.out.println("--------------------");
	        System.out.println("transformed housing features ");
	        System.out.println("--------------------");
	        
	        historicalHousingFeatures.select(column_name_med_inc,column_name_house_age,column_name_ave_rooms,column_name_ave_bedrms,column_name_population,column_name_AveOccup,column_name_lat,column_name_lon,column_name_price, column_name_features).show(30);
	        
	      //-------------------------------------------
	      // Use RandomForrest Regressor to fit the data
	      //-------------------------------------------
	      RandomForestRegressor randomForestRegressor = new RandomForestRegressor()
									      .setLabelCol(column_name_price)
									      .setFeaturesCol(column_name_features)
									      .setNumTrees(10); 
	      
	      // Train the model
	      RandomForestRegressionModel model = randomForestRegressor.fit(historicalHousingFeatures);
	
	      // Evaluate the model
	      RegressionEvaluator evaluator = new RegressionEvaluator()
	          .setLabelCol("label")
	          .setPredictionCol(column_name_price)
	          .setMetricName("rmse");
	      
	      
	      
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
	         Encoder<HouseInfo> houseInfoEncoder = Encoders.bean(HouseInfo.class);
	         Dataset<HouseInfo> houseBeanDS= houseDSAsString.map(new MapHouseInfoMapFunction(), houseInfoEncoder);
	         
	         
	         houseBeanDS.writeStream()
	         		.format("console")
	         		.start()
	         		.awaitTermination();

	         
	         
	         /*
	          
// Make predictions on the same data (or new data)
Dataset<Row> predictions = model.transform(data);

// Show predictions
predictions.select("features", "label", "prediction").show();

	          */
	         
	         
      } catch (Exception e) {
    	  e.printStackTrace();
      }
        
/*         

val query1 = ds1.collect.foreach(println)
  .writeStream
  .format("console")
  .start()
  
*/         
     //-------------------------------------------     
     // Select the key and value columns and cast them to String
     //-------------------------------------------     
//     Dataset<Row> processedStream = incomingHouseDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");   
//
//     processedStream.writeStream()
//                     .format("kafka") // Output to the console
//                     .option("kafka.bootstrap.servers", "localhost:9092")
//                     .option("topic", "output_data_topic")
//                     .option("checkpointLocation", "/tmp/sparkstreaming/checkpoint")
//                     .start() 
//                     .awaitTermination();
     
    }  //main

}
