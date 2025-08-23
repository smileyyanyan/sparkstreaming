### Apache Spark Analytics Applications

This repo contains analytics applications using Apache Spark. 

* **Project Outlier Detection with KMeans Clustering - Spark Batch**
* **Housing Price Prediction using Random Forest Regressor - Spark Streaming** 

### Project Outlier Detection with KMeans Clustering

This is a batch spark project that calculates outliers from a list of project management data stored in csv using the KMeans algorithm with z-score thresholding. 

Analysis Outline 
* Read the project management data from csv.
* Use Spark KMeans Clustering to create centers
* Assign each project to a center
* Calculate distance of each project to its assigned center
* Calculate mean and standard deviation for the distance
* Calculate z-score of each project
* Define z-score threshold for outliers
* Filter for the outliers.

Java run configurations 

--add-exports java.base/sun.nio.ch=ALL-UNNAMED  
--add-exports java.base/java.nio.ch=ALL-UNNAMED  
--add-opens=java.base/java.nio=ALL-UNNAMED  
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED  
--add-opens=java.base/java.util=ALL-UNNAMED  

### Housing Price Prediction with Random Forest Regressor

The house price prediction project first reads historical housing information from a CSV file. This information is used to predict house price of new houses on the market. Once the historical housing data has been fitted with a random forest regression model, the program awaits for new housing data from a kafka topic. As new houses are streamed from kafka, the historical data model is used to predict the price of each incoming house. 

##### Housing Price Prediction Design
![Alt text](HousingPricePredictorDesign.jpg?raw=true "Architectural Design")


##### Run Kafka

The incoming housing data in kafka can be produced from another application that manages houses on the market. The kafka console producer from kafka command line tools can be used to simulate incoming house messages. 

* Start kafka 
	.\kafka-server-start.bat ..\..\config\server.properties
	
* Use the kafka console producer command line tool to simulate producing data from REST endpoint.    
	kafka-console-producer --bootstrap-server localhost:9092 --topic input_topic_housing  
	
	>{"mediumIncome": 3.23,"houseAge": 23,"numberOfRooms": 6,"numberOfBedrooms": 3,"population": 325,"averageOccupation": 3.0,"latitude": 37.86,"longitude": -122.23}



##### Running Spark Streaming on Windows  

If your local development environment is on Windows, follow the steps below to install winutils.exe and hadoop.dll.

* Install Apache Hadoop.
* Make sure HADOOP_HOME is set.
* Reference https://github.com/cdarlint/winutils/tree/master for the appropriate hadoop version. 
* Download winutils.exe and hadoop.dll and add them to $HADOOP_HOME\bin.
* Add $HADOOP_HOME\bin to $PATH. 


