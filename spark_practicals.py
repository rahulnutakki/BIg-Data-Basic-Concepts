hadoop fs -put /home/keanesoft/srikanth_course/words_0712.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/words_0712.txt


Scala:
var textfile = sc.textFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/words_0712.txt")
var map_of_textfile = textfile.flatMap(line => line.split(" ")).map(word=>(word,1))
var counts_of_textfile = map_of_textfile.reduceByKey(_+_)
counts_of_textfile.take(5)


Pyspark:
rdd1 = sc.textFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/words_0712.txt")
counts = rdd1.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts_of_textfile.take(5)
counts.saveAsTextFile("hdfs://...")




path = 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pig_input/student.txt'

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/
hadoop fs -put /home/keanesoft/srikanth_course/student.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt
hadoop fs -put /home/keanesoft/srikanth_course/words2.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/words2.txt
hadoop fs -put /home/keanesoft/srikanth_course/employee.json hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/employee.json
hadoop fs -put /home/keanesoft/srikanth_course/employee2.json hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/employee3.json
hadoop fs -put /home/keanesoft/srikanth_course/employee4.json hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/employee4.json



hadoop fs -put /home/keanesoft/srikanth_course/new_words2.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/new_words2.txt
hadoop fs -mkdir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pig_input/
hdfs dfs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pig_input/student.txt


****************************************************************************************************************************************************************
****************************************************************************************************************************************************************
Jul 14th:
Difference between map and flamap:
data2.txt:
hadoop is fast
hive is sql on hdfs
spark is superfast
spark is awesome



hadoop fs -mkdir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/
hadoop fs -put data2.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/


Pyspark:
hdfs_path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/data2.txt"
data = sc.textFile(hdfs_path)
wc = data.map(lambda line:line.split(" "));
wc.collect()


fm = data.flatMap(lambda line:line.split(" "));
fm.collect()
temp_fm = fm.map(lambda temp: (temp, 1))
temp_fm2 = temp_fm.reduceByKey(lambda a, b: a + b)

************************************************************************************************
Date: 06/15
************************************************************************************************
Data Abstractions:
RDD's
Dataframes
Datasets


Creating RDD's:
1. parallelize
2. Reading the datasets from distributed storage system's.

1. parallelize
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
distData.getNumPartitions()
distData.take(10)
distData.collect(10)

distData.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2")

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00001
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00002
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00003
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00004
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00005
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00006
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/distData2/part-00007

words = sc.parallelize (
   ["word1",
   "word2",
   "word3",
   "word4",
   "word5",
   "word6",
   "word7",
   "word8",
   "word9",
   "new_word1",
   "new_word2",
   "new_word3",
   "new_word4",
   "new_word5"]
)

#example:
counts = words.count()
coll = words.collect()
print('count of words: ', counts)
print(coll)
****
#example foreach:
fore = words.foreach(f)  
def f(x):
    print(x)
****
filterout words:
words_filter = words.filter(lambda x: 'word' in x)
filtered = words_filter.collect()
print(filtered)
type(filtered)
type(words_filter)

****
#example k,v pair:
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print(mapping)
****
#example reduce function:
from operator import add
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print(adding)
type(nums)
type(adding)
****
#example
x = sc.parallelize([("spark", 1), ("hadoop", 4),("hadoop", 6)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print(final)


# Working with Key-Value Pairs
#example:
k,v pair:
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print(mapping)



Example for repartition and colasce:
----------------------------------------

repartition:
------------
data1 = [("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]
data2 = [("spark", 2), ("hadoop", 5),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]

rdd1 = sc.parallelize(data1)
rdd2 = sc.parallelize(data2)

joined_rdd = rdd1.join(rdd2)
# joined_rdd.collect()

# rdd1.getNumPartitions()
# rdd2.getNumPartitions()

joined_rdd_rep = joined_rdd.repartition(2)
joined_rdd_rep.getNumPartitions()

joined_rdd_rep.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/joined_rdd_rep")

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/joined_rdd_rep/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/joined_rdd_rep/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/joined_rdd_rep/part-00001

coalasce:
---------
data1 = [("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]
data2 = [("spark", 2), ("hadoop", 5),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]

rdd1 = sc.parallelize(data1)
rdd1.getNumPartitions()
Repartition:
rdd_rep = rdd1.repartition(4)
rdd_rep.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep_new")
Coalesce:
rdd_col = rdd1.coalesce(4)
rdd_col.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col_new")
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00001
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00002
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00003

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00001
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00002
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00003



rdd4 = sc.parallelize(data2)

joined_col_rdd = rdd3.join(rdd4)
joined_col_rdd.getNumPartitions()


tmp_rdd = joined_col_rdd.coalesce(2)
tmp_rdd.getNumPartitions()




Shuffle operations:
repartition operations like: repartition and coalesce,
key operations like: groupByKey and reduceByKey,
join operations like: cogroup and join.


#example:
words = sc.parallelize (
   ["word1",
   "word2",
   "word3",
   "word4",
   "word5",
   "word6",
   "word7",
   "word8",
   "word9",
   "new_word1",
   "new_word2",
   "new_word3",
   "new_word4",
   "new_word5"]
)
type(words)
words.getNumPartitions()
re_words = words.repartition(2)
re_words.getNumPartitions()



What is repartition and what is coalesce:
Difference between repartition and what is coalesce:

Keypoints of Repartition:
1. It does full shuffle in any scenario,
2. Will not use existing partitions,
3. Output is it makes all the partitions as equal sized partitions.


Repartition:
Before:
20 Partitions : 100GB(1GB, 15GB, 0.5GB,.....,1.2GB ===> Sum 100GB)
Apply repartition to 10
After:
10 Partitions :  (10GB,10GB,10GB....10GB ==> each Sum 100GB)


Keypoints of Coalesce:
1. It will try not to do full shuffle,
2. Will try to use existing partitions initially,
3. Output is, non equal sized partitions, when full shuffle is not done.

Coalesce:
Before:
20 Partitions : 100GB(6GB, 15GB, 11.5GB, 11GB, 13GB....,7GB ===> Sum 100GB)
Apply Coalesce to 10
After:
10 Partitions : (11.5GB,11GB, 13GB, 10GB,10GB..... ===> each Sum 100GB)



#example:
val distData = sc.parallelize(1 to 50)
distData.glom().collect(2)
distData.partitions.size
val new_dist = distData.repartition(2)
new_dist.partitions.size
new_dist.glom().collect()(0)


#example:
Lets keep all these statements in a script:
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/words.txt
lines = sc.textFile("/home/keanesoft/srikanth_course/student.txt")
lines = sc.textFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/words.txt")
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
type(counts)
counts.collect()
counts.saveAsTextFile("/home/keanesoft/srikanth_course/words_counts.txt")


Discussion on Spark-submit memory parameters:

Num of Nodes in cluster: 10 Nodes.
Each Node has : 16 cores.
Tot Capacity available in the cluster: 100GB.
# How much I want to give for my Spark App: 20GB.

Example of spark-submit command¸
spark-submit \
--driver-memory 3 \     ==> number of GBytes
--spark-driver-memory-overhead 1 \     ==> number of GBytes
--executor-memory-overhead 1 \     ==> number of GBytes\
--executor-cores 5 (I can use 3 executors per node) \ ==> number of cores per executor
--num-executors 10 \ ==> number of executors for entire cluster
--executor-memory 2 \ ==> memory per executor
--conf spark.dynamicAllocation.enabled=false \  ==> we can set any parameter exclusively for your application.
--packages com.databricks:spark-avro_2.11:3.2.0 \   ==> you can include any package like avro exclusively for your application.
--py-files <pythonScript> \ ==> your pyspark script
user_program_arguments  ==> arguments.



07/17 Class: Rahul
# conf = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
http://site.clairvoyantsoft.com/understanding-resource-allocation-configurations-spark-application/

************************************************************************************************************************
************************************************************************************************************************
Now we know what is spark context. But how do configure or get or set those parameters for our Spark Application?
# Fetch all conf properties Compare with different Pyspark shells.
sc._conf.getAll()
# Manage conf properties -
sc._conf.get("spark.master")
sc._conf.get("spark.app.name")
sc._conf.get("spark.executor.memory")
sc._conf.get("spark.dynamicAllocation.enabled")
sc._conf.get("spark.executor.cores")
sc._conf.get("spark.driver.memory")
sc._conf.get("spark.sql.shuffle.partitions")
sc._conf.setMaster("local").setAppName("My new app")
sc.stop()

sc._conf.setAll([('spark.executor.memory', '4g'), \
                ('spark.dynamicAllocation.enabled','false'),\
                ('spark.app.name', 'Spark Updated Conf'),\
                ('spark.executor.cores', '4'), \
                ('spark.cores.max', '4'), \
                ('spark.driver.memory','4g')])

sc._conf.setAll([('spark.dynamicAllocation.enabled','false')])


************************************************************************************************************************
************************************************************************************************************************
What is a SparkContext, SparkConf, "SparkSession", SQLContext, HiveContext.

Difference b/w SparkSession(>2.0 version) and sc.


# import SparkContext and then create sc in Script. Spark < 2.0 like 1.6 versions
from pyspark import SparkContext
from pyspark import SparkConf
sc = SparkContext("local", "Simple App")



# import SparkSession and create spark variable to access all context's. Spark >2.0 version.
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# The similar way that we did for spark 1.6 version above, we can also do Spark 2.0+ versions the below way.
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
SparkConf().getAll()
# Or without importing SparkConf:
spark.sparkContext.getConf().getAll()

('spark.driver.maxResultSize', '4g'),

SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','8g')])
SparkConf().setAll([('spark.driver.maxResultSize','2g')])
SparkConf().get('spark.driver.maxResultSize')
SparkConf().get('spark.master')
SparkConf().setAll([('spark.master','local')])

spark.sparkContext.getConf().getAll()
sc = SparkContext(conf=config)

# So in > 2.0 Use spark inplace of all context's.
Like:
spark.read.csv
spark.read.parquet
spark.read.json
spark.sql



pyspark --packages com.databricks:spark-csv_2.10:1.5.0
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt

1. df = sqlContext.load(header="true", path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
2. df = sqlContext.read.csv(header="true", path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df = sqlContext.read.load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")

spark.read.csv('hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt')


Spark - Web UI — Spark Application’s Web Console
https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-webui.html
************************************************************************************************************************
************************************************************************************************************************
# Why csv package is not working.
pyspark --packages com.databricks:spark-csv_2.11:1.5.0
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt
hadoop fs -put /home/keanesoft/srikanth_course/student3.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student3.txt
df = sqlContext.read.format("com.databricks.spark.csv") \
                .option("header", "false") \
                .option("inferSchema", "true") \
                .load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df.show()
df.printSchema()


Spark Dataframes:
# CSV to Spark DF w/o schema attached.
pyspark --packages com.databricks:spark-csv_2.11:1.5.0
df1 = sqlContext.read.format("com.databricks.spark.csv") \
            .option("header", "false") \
            .option("inferSchema", "true") \
            .load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df1.printSchema()

df2 = sqlContext.read.format("com.databricks.spark.csv") \
            .option("header", "false") \
            .option("inferSchema", "false") \
            .load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df2.printSchema()

df3 = sqlContext.read.format("com.databricks.spark.csv") \
            .option("header", "false") \
            .load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df3.printSchema()

df4 = sqlContext.read.format("com.databricks.spark.csv") \
            .load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df4.printSchema()



header default is false
inferSchema default is false ===> default values of columns is string.


df3.show()
df3.printSchema()


************************************************************************************************************
Jul 19th:
************************************************************************************************************
from pyspark.sql.types import *
student_schema = StructType([
                            StructField("Student_id", StringType()),
                            StructField("Student_name", StringType()),
                            StructField("Student_city", StringType())])

# 1st Approach
# CSV to Spark DF with schema attached.
df4 = spark.read.format("com.databricks.spark.csv") \
            .option("header", "false") \
            .option("schema",student_schema) \
            .option("inferSchema", "true") \
            .load("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")


# 2nd Approach
rdd = sc.textFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt")
df2 = spark.createDataFrame(rdd,student_schema) # This is the syntax.
df4.show()
df4.printSchema()
df2.printSchema()

# 3rd Approach
# >>> In Databricks > 2.0 version
df1 = spark.read.csv('/FileStore/tables/student.txt')
print('***********dataframe without schema***********')
df1.show()
df1.printSchema()
print('***********dataframe without schema***********')
student_schema = StructType([
        StructField("Student_id", StringType()),
        StructField("Student_name", StringType()),
        StructField("Student_city", StringType())])

hdfs_path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt"
df2 = spark.read.csv('/FileStore/tables/student.txt', schema=student_schema,header=False, sep=',')
df2 = spark.read.csv(hdfs_path, schema=student_schema,header=False, sep=',')
df3 = spark.read.csv(hdfs_path, schema=student_schema,header=False, sep='^')
df4 = spark.read.csv(hdfs_path, schema=student_schema,header=False, sep='$')
df5 = spark.read.csv(hdfs_path, schema=student_schema,header=False)

hdfs_path_multi = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student_multi.txt"
df6 = spark.read.csv(hdfs_path_multi, schema=student_schema,header=False,multiLine = True)
df7 = spark.read.csv(hdfs_path_multi, schema=student_schema,header=False,multiLine = False)


hdfs_path2 = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student_multi2.txt"
df8 = spark.read.csv(hdfs_path2, schema=student_schema,header=False,multiLine = True)


# Options - Schema, Header, Sep, Inferschema, multiLine
print('>>>>***********dataframe with schema***********')
df2.show()
df2.printSchema()
print('>>>>>***********dataframe without schema***********')

hadoop fs -put /home/keanesoft/srikanth_course/student_new2.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student_new2.txt
path = 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student_new.txt'
path2 = 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student_new2.txt'

dfy = spark.read.csv(path, schema=student_schema,header='true', sep=',')
dfz = spark.read.csv(path, schema=student_schema,header='true')

dfa = spark.read.csv(path2, schema=student_schema,header='true', sep='^')
dfb = spark.read.csv(path2, schema=student_schema,header='true')
dfa.show()
dfb.show()


************************************************************************************************************************
************************************************************************************************************************
Data Manupulations:
# Filter, createOrReplaceTempView:
df3 = df2.where(df2['Student_id'] >= '990')
df3.createOrReplaceTempView('df3_temp_view') #registerTempTable
spark.sql('select * from df3_temp_view').show()

# Save options - Default Values
df3.select("Student_id", "Student_city").write.save("filterd_student_list_parquet.parquet")

# overwrite,append - Default Values
df3.select("Student_id", "Student_city").write.mode('overwrite').parquet("filterd_student_list_parquet3.parquet")
spark.read.parquet("filterd_student_list_parquet").show()


# save format's - Default Values
df3.select("Student_id", "Student_city").write.mode('append').format('json').save("filterd_student_list_json")
df3.select("Student_id", "Student_city").write.mode('append').format('avro').save("filterd_student_list_avro")
df3.select("Student_id", "Student_city").write.mode('append').save("zfilterd_student_list_parquet")
df3.select("Student_id", "Student_city").write.mode('append').format('avro').save("zfilterd_student_list_avro")
spark.read.json("filterd_student_list.json").show()

# save to avro
df3.write.avro("filterd_student_list.avro")


spark.read.json('source_json_path')
spark.read.avro('source_avro_path')
********************************** CONTINUE FROM HERE **********************************
********************************** CONTINUE FROM HERE **********************************
# Add column withColumn and withColumnRenamed

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/student_new2.txt
hadoop fs -mkdir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/
hadoop fs -put /home/keanesoft/srikanth_course/student_new2.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/

from pyspark.sql.types import *
student_schema = StructType([
        StructField("Student_id", IntegerType()),
        StructField("Student_name", StringType()),
        StructField("Student_city", StringType())])

hdfs_path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/student_new2.txt"
df1 = spark.read.csv(hdfs_path, schema=student_schema,header=True, sep='^')
df1.printSchema()
df1.createOrReplaceTempView('categories')

df1.show()



df2 = df1.withColumn('Student_id_new', df1.Student_id + 1000)
df2.printSchema


df3 = df2.withColumnRenamed('Student_city', 'Student_city_rename')
df3.show()

df4 = df2.withColumn('Student_city_new_col', concat(df1.Student_id,'text'))
df4.printSchema


from pyspark.sql.functions import *
student_status_check_udf = udf(lambda Student_id: "new" if Student_id >=6 else "old", StringType())

df_udf = df1.withColumn("student_status_check", student_status_check_udf(df1.Student_id))
df_udf.show()

df_udf2 = df1.withColumn("student_temp_col", lit('place_holder'))
df_udf2.show()

df_udf.createOrReplaceTempView('df_udf')
Approach2 of using sql functions:
spark_sql_df = spark.sql("""select Student_id, Student_name, Student_city, student_status_check, \
                concat(Student_city,'value') as concatenated_newcol from df_udf""")




df_udf3 = df_udf.withColumn('LOADTIMESTAMP',from_utc_timestamp ( current_timestamp (), "PST" ) )
df_udf3.show()



# Casting
df_udf4 = df_udf3.withColumn('Student_id_cast', (df_udf3.Student_id + 1000).cast("string"))
df_udf4.show()
df_udf4.printSchema()


df_udf5 = df_udf3.withColumn('Student_name_cast', (df_udf3.Student_name).cast("int"))
df_udf5.show()
df_udf5.printSchema()


Approach1:
def spark_concat1(inp_var):
    out_var = inp_var + 'text'
    return out_var
spark_concat1_udf1 = udf(spark_concat1, StringType())
concat_udf_df1 = df1.withColumn("student_concat", spark_concat1_udf1(df1['Student_name']))
concat_udf_df1.show()
concat_udf_df1.printSchema()


Approach3:
spark.udf.register("spark_concat1_udf2", spark_concat1, StringType())

# concat_udf_df2 = df1.withColumn("student_concat", spark_concat1_udf2(df1['Student_name']))
# concat_udf_df2 = df1.withColumn("student_concat", spark_concat1_udf2(df1.Student_name))
concat_udf_df2.show()
concat_udf_df2.printSchema()

************************************************************************************************************************
************************************************************************************************************************
Using SparkSQL:
pyspark --packages com.databricks:spark-csv_2.10:1.5.0
from pyspark.sql.types import *
student_schema = StructType([
                            StructField("Student_id", StringType()),
                            StructField("Student_name", StringType()),
                            StructField("Student_city", StringType())])

hdfs_path = 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark/student.txt'

df1 = sqlContext.read.format("com.databricks.spark.csv") \
            .option("header", "false") \
            .option("schema",student_schema) \
            .option("inferSchema", "true") \
            .load(hdfs_path)

df1.where
df1.show()
df1.registerTempTable('df1_temp_table')
df2 = sqlContext.sql("""select * from df1_temp_table""")
****************************************************************************************************************************************************************
****************************************************************************************************************************************************************
Project with a simple Dataset :
------------------------------
Column no. 1: Video id.
Column no. 2: Video uploader.
Column no. 3: Interval between the day of establishment of YouTube and the date of uploading of the video.
Column no. 4: Category of the video.
Column no. 5: Length of the video.
Column no. 6: Number of views for the video.
Column no. 7: Rating on the video.
Column no. 8: Number of ratings given for the video
Column no. 9: Number of comments on the videos.
Column no. 10: Related video ids of the uploaded video.


schema1 = StructType([
    StructField("f1", StringType(), True),
    StructField("f2", StringType(), True),
    StructField("f3", DoubleType(), True),
    StructField("f4", StringType(), True),
    StructField("f5", DoubleType(), True),
    StructField("f6", DoubleType(), True),
    StructField("f7", DoubleType(), True),
    StructField("f8", DoubleType(), True),
    StructField("f9", DoubleType(), True),
    StructField("f10", StringType(), True),
    StructField("f11", StringType(), True),
    StructField("f12", StringType(), True),
    StructField("f13", StringType(), True),
    StructField("f14", StringType(), True),
    StructField("f15", StringType(), True),
    StructField("f16", StringType(), True),
    StructField("f17", StringType(), True),
    StructField("f18", StringType(), True),
    StructField("f19", StringType(), True),
    StructField("f20", StringType(), True),
    StructField("f21", StringType(), True),
    StructField("f22", StringType(), True),
    StructField("f23", StringType(), True)])

hadoop fs -put /home/keanesoft/srikanth_course/youtube_new_data.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/youtube_new_data.txt
hdfs_path = 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark2/youtube_new_data.txt'

df1 = spark.read.csv(hdfs_path, schema=schema1,header=False, sep="\t")
df1.printSchema()
df1.show()
df1.createOrReplaceTempView("df1")
spark.sql("""select f1,f22,f23 from df1""").show()
df2 = df1.withColumn('f23_alias', df1.f23)


df1.createOrReplaceTempView("df1")
dfx = spark.sql("""select f1,f3,f5 from df1""")

dfx.createOrReplaceTempView("dfx")
dfx.show()
dfx.printSchema()

dfy = spark.sql("""select f1, max(dfx.f3), max(dfx.f5) from dfx group by dfx.f1""")
dfy.show()
dfy.printSchema()
dfy.rdd.getNumPartitions()


df2 = spark.sql('''select vdo_category, COUNT(vdo_category) category_count from col_4 group by vdo_category order by category_count DESC''')
df2.coalesce(1).write.option("delimiter",",").option("header", "true").mode("overwrite").csv('s3://Bucket_Name/xxxx-xxxx/xxxx/spark/TopCategory')

****************************************************************************************************************************************************************
****************************************************************************************************************************************************************
Spark with UDF:
--------------
from pyspark.sql.functions import udf
student_status_check_udf = udf(lambda Student_id: "new" if Student_id >=9000 else "old", StringType())

df1.withColumn("student_status_check", student_status_check_udf(df1.Student_id))

****************************************************************************************************************************************************************
****************************************************************************************************************************************************************
Spark with hive:

sqlContext.sql("""show databases""").show(20,False)

sqlContext.sql("""use srikanth_course""").show(20,False)
sqlContext.sql("""show create table srikanth_course.employee_table3""").show(50,False)
sqlContext.sql("""show create table srikanth_course.employee_table4""").show(50,False)


sqlContext.sql("""select * from srikanth_course.employee_table3""").show(20,False)
sqlContext.sql("""select * from srikanth_course.employee_table4""").show(20,False)
sqlContext.sql("""select * from srikanth_course.employee_table3""").count()
sqlContext.sql("""select * from srikanth_course.employee_table4""").count()
sqlContext.sql("""select * from srikanth_course.employee_table4 where dateofjoin='1999'""").show(10,False)


sqlContext.sql("""show partitions srikanth_course.employee_table4""").show()



SparkConf().setAll([('hive.exec.dynamic.partition.mode','nonstrict')])
-- Insert  the data
sqlContext.sql("hive.exec.dynamic.partition.mode","nonstrict")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
sqlContext.sql("""
                INSERT INTO TABLE srikanth_course.employee_table4
                PARTITION (dateofjoin)
                select * from srikanth_course.employee_table3""")

sqlContext.sql("""INSERT INTO TABLE srikanth_course.employee_table3 VALUES ('999','name999', '100000', 'Mgr', '1998')""")

sqlContext.sql("""select * from srikanth_course.employee_table3""").show(20,False)
sqlContext.sql("""show partitions srikanth_course.employee_table4""").show(20,False)


sqlContext.sql("""ALTER TABLE srikanth_course.employee_table4 DROP IF EXISTS PARTITION (dateofjoin=1998)""")


Example:

employee_df = spark.sql("""select * from srikanth_course.employee_table3""")
employee_df.registerTempTable('employee_df')
spark.sql("""select distinct designation from srikanth_course.employee_table3""").show()

spark.sql("""show create table srikanth_course.employee_table3""").show(100,False)
spark.sql("""show tables""").show(20,False)
spark.sql("""
            CREATE EXTERNAL TABLE `srikanth_course`.`desg_table`(`desg_id` string, `desg_name` string)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            WITH SERDEPROPERTIES (
              'line.delim' = '
            ',
              'field.delim' = ',',
              'serialization.format' = ','
            )
            STORED AS
              INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
              OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
              LOCATION 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/desg_table'
        """)


spark.sql("""INSERT INTO TABLE srikanth_course.desg_table VALUES ('1','Admin99')""")
spark.sql("""INSERT INTO TABLE srikanth_course.desg_table VALUES ('2','Admin108')""")




employee_df = spark.sql("""select * from srikanth_course.employee_table3""")
desg_df = spark.sql("""select * from srikanth_course.desg_table""")


employee_df.registerTempTable('employee_df')
desg_df.registerTempTable('desg_df')

employee_df.printSchema()
desg_df.printSchema()
employee_df.count()
desg_df.count()


join_df = spark.sql("""
                    select e.*,d.* from employee_df e left outer join desg_df d on e.designation = d.desg_name
                    """)

join_df.show()
join_df.count()
