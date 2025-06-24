This application uses Spark Structured Streaming to read messages from a kafka input topic and writes it to an output kafka topic. 


##### Running the Applications

Before starting the applications, make sure kafka server is started. 
.\kafka-server-start.bat ..\..\config\server.properties

All applications are configured to use spring.kafka.bootstrap-servers locally at localhost:9092.

* First start the spring boot application 
* Then start the spark streaming application.
* Through spring boot, end a message to the input kafka topic.
* The spark readstream should pick up the new message from the input kafka topic and saves it to the output topic.
* The spring boot application KafkaConsumerService should pick up the message from the output topic and print it to the console. 


##### Running on Windows  

* Install Apache Hadoop.
* Make sure HADOOP_HOME is set.
* Download winutils.exe and hadoop.dll and add them to $HADOOP_HOME\bin.
* Add $HADOOP_HOME\bin to $PATH. 


