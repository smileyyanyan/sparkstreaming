

##### Housing Price Prediction Design
![Alt text](HousingPricePredictorDesign.jpg?raw=true "Architectural Design")



##### Run Kafka

* Start kafka 
	.\kafka-server-start.bat ..\..\config\server.properties
	
* Use the kafka console producer command line tool to simulate producing data from REST endpoint.    
	kafka-console-producer --bootstrap-server localhost:9092 --topic input_topic_housing  
	
	>{"mediumIncome": 3.23,"houseAge": 23,"numberOfRooms": 6,"numberOfBedrooms": 3,"population": 325,"averageOccup": 3.0,"latitude": 37.86,"longitude": -122.23}



##### Running on Windows  

* Install Apache Hadoop.
* Make sure HADOOP_HOME is set.
* Reference https://github.com/cdarlint/winutils/tree/master for the appropriate hadoop version. 
* Download winutils.exe and hadoop.dll and add them to $HADOOP_HOME\bin.
* Add $HADOOP_HOME\bin to $PATH. 


