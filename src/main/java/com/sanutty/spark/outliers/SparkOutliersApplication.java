package com.sanutty.spark.outliers;

import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkOutliersApplication
{
  private static final String project_id_column_name = "projectid";
  private static final String status_indicator_column_name = "project_status";
  private static final String percent_complete_column_name = "percent_complete";
  private static final String project_duration_days_column_name = "project_duration_days";
  private static final String labor_actual_column_name = "labor_actual";
  private static final String days_elapsed_column_name = "days_elapsed";
  private static final String labor_basesum_column_name = "labor_basesum";
  private static final String labor_estimated_column_name = "labor_estimated";
  private static final String team_count_column_name = "teamcount";
  private static final String task_count_column_name = "taskcount";
  
  private static final String distanceFunctionName = "calculateSquareDistance";
  private static final String zScoreFunctionName = "calculateZScore";
  
  
  
  private static final String distance_column_name = "distance";
  private static final String mean_column_name = "mean";
  private static final String standard_deviation_column_name = "stddev";
  private static final String zscore_column_name = "zscore";
  private static final String features_column_name = "features";
  
  private static final String center_id_column_name = "prediction";
  private static final String center_vector_column_name = "center";
  
  private static final String csvPath = "src/main/resources/projectdata.csv";
  
  public static void main( String[] args )
  {
    Dataset<Row> projectDataset = null;
    
    //-------------------------------------------
    // Start spark
    //-------------------------------------------
    SparkSession spark = SparkSession
                        .builder()
                        .appName("ProjectOutliers")
                        .master( "local[*]" )
                        .getOrCreate();
    
    //-------------------------------------------
    // define an explicit schema for the csv to be read 
    //-------------------------------------------
    StructField structFieldProjectId = new StructField(project_id_column_name, DataTypes.IntegerType, false, Metadata.empty());
    StructField structFieldStatus = new StructField(status_indicator_column_name, DataTypes.IntegerType, false, Metadata.empty());
    StructField structFieldPercentComplete = new StructField(percent_complete_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structFieldProjectDurationDays = new StructField(project_duration_days_column_name, DataTypes.IntegerType, false, Metadata.empty());
    StructField structField_labor_eacsum = new StructField(labor_actual_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structField_Days_elapsed = new StructField(days_elapsed_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structField_labor_basesum = new StructField(labor_basesum_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structField_labor_etcsum = new StructField(labor_estimated_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structField_team_count = new StructField(team_count_column_name, DataTypes.IntegerType, false, Metadata.empty());
    StructField structField_task_count = new StructField(task_count_column_name, DataTypes.IntegerType, false, Metadata.empty());
    
    StructField[] csvSchemafields = new StructField[] {
        structFieldProjectId,
        structFieldStatus,
        structFieldPercentComplete,
        structFieldProjectDurationDays,
        structField_Days_elapsed,
        structField_labor_etcsum,
        structField_labor_eacsum,
        structField_labor_basesum,
        structField_team_count,
        structField_task_count
    };
    StructType csvSchema = new StructType(csvSchemafields);
    
    //-------------------------------------------
    // read the project csv data using the schema
    //-------------------------------------------
    projectDataset = spark.read()
                    .option("delimiter", ",")
                    .option("header", "true")
                    .schema( csvSchema )  
                    .csv(csvPath);
        
    System.out.println("--------------------");
    System.out.println("Projects from csv");
    System.out.println("--------------------");
    
    projectDataset.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    //projectid,status_indicator,percent_complete,project_duration_days,days_elapsed,labor_etcsum,labor_eacsum,labor_basesum,teamcount,taskcount
    System.out.println("--------------------");
    System.out.println("Project from db schema");
    System.out.println("--------------------");
    projectDataset.printSchema();
    
    //-------------------------------------------
    // Define kmeans centers - 6
    //-------------------------------------------
    // Trains a k-means model.

    //Define features fields   
    VectorAssembler assembler = new VectorAssembler()
        .setInputCols(new String[]{ status_indicator_column_name, 
                                    percent_complete_column_name, 
                                    project_duration_days_column_name, 
                                    days_elapsed_column_name, 
                                    labor_estimated_column_name, 
                                    labor_actual_column_name, 
                                    labor_basesum_column_name, 
                                    team_count_column_name, 
                                    task_count_column_name})
        .setOutputCol("features");

    Dataset<Row> projectFeatures = assembler.transform(projectDataset);
    
    System.out.println("--------------------");
    System.out.println("transformed project features ");
    System.out.println("--------------------");
    
    projectFeatures.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    KMeans kmeans = new KMeans().setK(6).setSeed(1L);
    KMeansModel model = kmeans.fit(projectFeatures);
    
    Vector[] clusterCenters = model.clusterCenters();
    
    //-------------------------------------------
    // Assign a center to each project
    //------------------------------------------- 
    Dataset<Row> predictions = model.transform(projectFeatures);
    
    System.out.println("--------------------");
    System.out.println("Projects with center");
    System.out.println("--------------------");
    
    predictions.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });

    //-------------------------------------------
    // Register a distance function and calculate distance
    //------------------------------------------- 
    spark.udf().register(
        distanceFunctionName, // name of function
        (UDF2<Vector, Integer, Double>) 
        (feature, centerIndex) -> 
        {  
          Vector center = clusterCenters[centerIndex];
          return Double.valueOf(Vectors.sqdist(feature, center));
        }, // all convert rules
        DataTypes.DoubleType // return type
     );
    predictions = predictions.withColumn( distance_column_name, functions.callUDF(distanceFunctionName, functions.col(features_column_name), functions.col(center_id_column_name)));
    
    System.out.println("--------------------");
    System.out.println("Predictions with distance");
    System.out.println("--------------------");
    
    predictions.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    //-------------------------------------------
    // Calculate mean and standard deviation
    //-------------------------------------------
    Dataset<Row> statsDF = predictions.select(functions.mean(functions.col(distance_column_name)));
    List<Row> statsList = statsDF.collectAsList();
    
    System.out.println("--------------------");
    System.out.println("All distances");
    System.out.println("--------------------");
    System.out.println(statsList);
    
    Row statsRow = statsList.get(0);
    double mean = Double.parseDouble( statsRow.get(0).toString());
    predictions = predictions.withColumn( mean_column_name, functions.lit(mean));
    
    System.out.println("--------------------");
    System.out.println("Add mean as column to predictions");
    System.out.println("--------------------");
    predictions.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    statsDF = predictions.select(functions.stddev(functions.col(distance_column_name)));
    statsList = statsDF.collectAsList();
    statsRow = statsList.get(0);
    double standardDeviation = Double.parseDouble( statsRow.get(0).toString());
    
    predictions = predictions.withColumn( standard_deviation_column_name, functions.lit(standardDeviation));
    
    System.out.println("--------------------");
    System.out.println("Predictions with mean and stddev");
    System.out.println("--------------------");
    
    predictions.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    //-------------------------------------------
    // Calculate z-score for the distance column
    //-------------------------------------------
    spark.udf().register(
        zScoreFunctionName, // name of function
        (UDF3<Double, Double, Double, Double>) 
        (distance, average, stdDev) -> 
        {  
         return (distance-average) / stdDev;
        }, 
        DataTypes.DoubleType 
     );    
    predictions = predictions.withColumn( zscore_column_name, functions.callUDF(zScoreFunctionName, functions.col(distance_column_name), functions.col(mean_column_name), functions.col(standard_deviation_column_name)));

    System.out.println("--------------------");
    System.out.println("Predictions with zscore");
    System.out.println("--------------------");
    
    predictions.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    double zscoreThreshold = 2.0;
    //-------------------------------------------
    // filter out outlier records with over the threshold z-score. 
    //-------------------------------------------
    Dataset<Row> projectOutliers = predictions.filter((FilterFunction<Row>) row -> {
        Object z = row.getAs(zscore_column_name);
        double zScore = Double.parseDouble( z.toString() );
        return Math.abs(zScore) > zscoreThreshold;
    });
    
    System.out.println("--------------------");
    System.out.println("Project outliers schema");
    System.out.println("--------------------");
    projectOutliers.printSchema();

    System.out.println("--------------------");
    System.out.println("Project outliers");
    System.out.println("--------------------");
    
    projectOutliers.foreach((ForeachFunction<Row>) row -> {
      System.out.println(row);
    });
    
    //-------------------------------------------
    // Save outliers 
    //-------------------------------------------
    
    StructField structFieldFeature = new StructField(features_column_name, DataTypes.StringType, false, Metadata.empty());
    StructField structFieldDistance = new StructField(distance_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structFieldCenterVector = new StructField(center_vector_column_name, DataTypes.StringType, false, Metadata.empty());
    
    StructField structFieldMean = new StructField(mean_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structFieldStdDev = new StructField(standard_deviation_column_name, DataTypes.DoubleType, false, Metadata.empty());
    StructField structFieldZscore = new StructField(zscore_column_name, DataTypes.DoubleType, false, Metadata.empty());
    
    //Define schema to match database table columns
    StructField[] dbSchemafields = new StructField[] {
        structFieldProjectId,
        structFieldFeature,
        structFieldDistance,
        structFieldCenterVector,
        structFieldMean,
        structFieldStdDev,
        structFieldZscore,
     };
    
    StructType outlierDBSchema = new StructType(dbSchemafields);
    
    //Select only columns in the database table
    projectOutliers = projectOutliers.map(new MapFunction<Row, Row>() {
      private static final long serialVersionUID = 1L;

      @Override
      public Row call(Row row) {
        Object projectid = row.getAs(project_id_column_name);
        Object feature = row.getAs(features_column_name).toString();
        Object distance = row.getAs(distance_column_name);
        Object mean = row.getAs(mean_column_name);
        Object stddev = row.getAs(standard_deviation_column_name);
        Object zscore = row.getAs(zscore_column_name);
        
        int centerIndex = Integer.parseInt(row.getAs("prediction").toString()); 
        Vector center = clusterCenters[centerIndex];
        //Row newRow = new GenericRowWithSchema(values, newSchema);
        Object[] values = {projectid, feature, distance, center.toString(), mean, stddev, zscore};
        
        return new GenericRowWithSchema(values, outlierDBSchema);
      }
    }, Encoders.row(outlierDBSchema));

    System.out.println("--------------------");
    System.out.println("Outliers table schema");
    System.out.println("--------------------");
    projectOutliers.printSchema();
    
    System.out.println("--------------------");
    System.out.println("Outliers found");
    System.out.println("--------------------");
    
    projectOutliers.foreach( (ForeachFunction<Row>)  row -> {
      System.out.println(row);
    });
    
    spark.stop();
    
  } //main

}
