from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split, udf, col
import mysql.connector
import re

sc = SparkContext(appName = "test")
print("Helolo")
from pyspark.sql import SparkSession   
spark = SparkSession.builder.getOrCreate()

hostname = "localhost"
user = "root"
password = "99tkoffice" # whatever you have set as your password in mysql
database = "dbt" # or whatever you named your database in mysql
table_name = "tweets"


def drop_table_if_exists(tablename):
	db = mysql.connector.connect(host=hostname, user=user, password=password, database=database)
	cursor = db.cursor()
	sql = f"DROP TABLE IF EXISTS {tablename}"
	cursor.execute(sql)
	
	db.close()
	

def lowercase(s):
	return s.lower()
	
def remove_digits(s):
	return re.sub(r'[0-9]', '', s)
	
def remove_punctuations(s):
	return re.sub(r'[^\w\s]', '', s)


@udf(returnType = StringType())
def extract_hashtags(s):
	return re.findall(r'#(\w+)', s)


@udf(returnType = StringType())
def preprocess(s):
	news = lowercase(s)
	news = remove_digits(news)
	news = remove_punctuations(news)
	
	return news

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "tweets, sentiments") \
  .option("startingOffsets", "earliest") \
  .option("includeHeaders", "true") \
  .load()

query = df.selectExpr("CAST(value AS STRING)")
df1 = query.withColumn('sentiment', split(query['value'], '\s+', limit=2).getItem(0)).withColumn('tweet', split(query['value'], '\s+', limit=2).getItem(1))

db_target_properties = {"user":user, "password":password}

def foreach_batch_function(df, epoch_id):
    df.write.mode("append").jdbc(url=f'jdbc:mysql://localhost:3306/{database}',  table=table_name,  properties=db_target_properties)



data = df1.select("sentiment", "tweet")
data = data.withColumn("hashtags", extract_hashtags(col("tweet")))
data = data.withColumn("preprocess", preprocess(col("tweet")))

drop_table_if_exists(table_name)
out = data.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()

out.awaitTermination()


