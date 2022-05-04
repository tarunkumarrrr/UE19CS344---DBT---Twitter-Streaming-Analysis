# UE19CS344---DBT---Twitter-Streaming-Analysis


install hadoop and spark.
install kafka using the tutorial here: https://www.youtube.com/watch?v=hyJZP-rgooc

Go to the following directories and run these commands

cd $HADOOP_HOME

./sbin/start-all.sh

cd $SPARK_HOME

./sbin/start-all.sh

cd $KAFKA_HOME

./bin/zookeeper-server-start.sh config/zookeeper.properties

./bin/kafka-server-start.sh config/server.properties

To check if kafka is consuming data correctly(OPTIONAL):
cd $KAFKA_HOME
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning -topic tweets

Go to the directory containing produce_tweets.py
Run this in another console:

python3 produce_tweets.py

Open another console and run the following command for spark AND MAKE SURE TO CHANGE PATH OF read_kafka_spark.py and error.txt

./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-class-path /usr/share/java/mysql-connector-java-8.0.29.jar --jars /usr/share/java/mysql-connector-java-8.0.29.jar /home/pes1ug19cs222/Desktop/PES/DBT/DBT_Project/read_kafka_spark.py 2> /home/pes1ug19cs222/Desktop/PES/DBT/DBT_Project/output/error.txt

With another console, you can check the mysql table:
Starting mysql:
mysql -u root -p

Enter password upon prompt.

Delete the topic (The next time you restart the entire process):

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic tweets


For stream and batch processing:
Download Confluent Kafka
https://linuxtut.com/en/be38fd4e080b188086e9/
 

Startup kafka:
cd $KAFKA_HOME

./bin/zookeeper-server-start.sh config/zookeeper.properties

./bin/kafka-server-start.sh config/server.properties

Run Producer Python File:
python3 producer_tweet.py

Run Consumer Python Files:
For Batch: python3 consumer_in_batch.py 
For Stream: python3 consumer.py
