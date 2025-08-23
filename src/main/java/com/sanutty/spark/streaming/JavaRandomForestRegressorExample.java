package com.sanutty.spark.streaming;

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

import com.sanutty.spark.streaming.entities.HouseInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JavaRandomForestRegressorExample {

    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaRandomForestRegressorExample")
                .master("local[*]")
                .getOrCreate();

        // 1. Prepare the training data
        // For a real-world scenario, you would load data from a file (e.g., CSV).
        // Here we create a synthetic dataset.
        
        List<House> data = Arrays.asList(
                new House(250.0, 3.0, 1995.0, 350000.0),
                new House(300.0, 4.0, 2005.0, 450000.0),
                new House(150.0, 2.0, 1980.0, 250000.0),
                new House(400.0, 5.0, 2010.0, 550000.0)
        );

        Encoder<House> houseEncoder = Encoders.bean(House.class);
        Dataset<House> trainingData = spark.createDataset(data, houseEncoder);
        
        trainingData.show();
        
        // 2. Assemble the features into a single vector column
        // RandomForestRegressor expects a single "features" column that is a vector.
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"sqft", "bedrooms", "yearBuilt"})
                .setOutputCol("features");

        // 3. Create the RandomForestRegressor model
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("price")
                .setFeaturesCol("features")
                .setNumTrees(10); // Number of trees in the forest

        // 4. Create a Pipeline to chain the feature assembler and the model
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{vectorAssembler, rf});

        // 5. Train the model
        System.out.println("Training the model...");
        PipelineModel model = pipeline.fit(trainingData);

        // 6. Prepare new data for prediction
        List<House> newHouseData = Arrays.asList(
                new House(350.0, 4.0, 2012.0, 0.0), // Price is unknown
                new House(180.0, 2.0, 1990.0, 0.0)  // Price is unknown
        );
        
        Dataset<House> testData = spark.createDataset(newHouseData, houseEncoder);
        
        testData.show();
                
        // 7. Make predictions on the test data
        Dataset<Row> predictions = model.transform(testData);
        
        // 8. Show the predictions
        System.out.println("Predictions:");
        predictions.select("features", "price", "prediction").show();

        spark.stop();
    }

    // A simple POJO for the house data.
    public static class House implements java.io.Serializable {
        private double sqft;
        private double bedrooms;
        private double yearBuilt;
        private double price;
        
        public House(double sqft, double bedrooms, double yearBuilt) {
        	this(sqft, bedrooms, yearBuilt, -1);
        }

        
        public House(double sqft, double bedrooms, double yearBuilt, double price) {
            this.sqft = sqft;
            this.bedrooms = bedrooms;
            this.yearBuilt = yearBuilt;
            this.price = price;
        }

        public double getSqft() { return sqft; }
        public void setSqft(double sqft) { this.sqft = sqft; }
        public double getBedrooms() { return bedrooms; }
        public void setBedrooms(double bedrooms) { this.bedrooms = bedrooms; }
        public double getYearBuilt() { return yearBuilt; }
        public void setYearBuilt(double yearBuilt) { this.yearBuilt = yearBuilt; }
        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }
    }
}

