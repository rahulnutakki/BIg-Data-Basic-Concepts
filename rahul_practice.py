------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Path of the rahuk.txt file
/home/keanesoft/rahul_hadoop/rahul.txt

/home/keanesoft/rahul_hadoop/test.txt

To find the path we can use command

realpath nameOfFile.text

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/rahul_test


hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/rahul_test

the above command gives result as the it is a directory


hadoop fs -put /home/keanesoft/srikanth_course/test_car.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/rahul_test





####### To make a directory in hadoop

hadoop fs -mkdir NAMe 

hadoop fs -mkdir rahul_hadoop

hadoop fs -put /home/keanesoft/srikanth_course/test.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/employee_test/




hadoop fs -put /path/in/linux /hdfs/path

hadoop fs -put /home/keanesoft/rahul_hadoop/rahul.txt /hdfs/path


hadoop dfs -copyFromLocal <local-dir> <hdfs-dir>



hadoop fs -put //home/keanesoft/rahul_hadoop/rahul.txt hdfs://nn01.itversity.com:8020/rahul_hadoop/

drop table rahulnutakki.rahul_test;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE Table IF NOT EXISTS rahulnutakki.rahul_test (
eid string,
name String,
salary String,
designation String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;



CREATE Table IF NOT EXISTS rahulnutakki.rahul_testx (
eid string,
name String,
salary String,
designation String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE Table IF NOT EXISTS rahulnutakki.rahul_testy (
eid string,
name String,
salary String,
designation String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

show create table rahul_test;

select * from rahulnutakki.rahul_test;

1201,Gopal,45000,Technicalmanager       
1202,Manisha,45000,Proofreader  
1203,Masthanvali,40000,Technicalwriter  
1204,Kiran,40000,HrAdmin        
1205,Kranth,430000,OpAdmin
1206,Gokul,46000,SystemAdmin       
1207,Ranga,47000,DotNet  
1208,Rahul,48000,DataScientist  
1209,Vamsi,410000,Android        
12010,Nehanth,430000,SupplyChain  

-- For table X
1201,prashanth,45000,ElectricalEngineer       
1202,viswa,45000,PHD  
1203,manminder,40000,DataAnalyst  
1204,shikha,40000,JrSE        
1205,ayushi,430000,JrSE2    

-- For Table Y
1206,Gokul,46000,SystemAdmin       
1207,Ranga,47000,DotNet  
1208,Rahul,48000,DataScientist  
1209,Vamsi,410000,Android        
12010,Nehanth,430000,SupplyChain 



1201,Gopal,45000,Technicalmanager       
1202,Manisha,45000,Proofreader  
1203,Masthanvali,40000,Technicalwriter  
1204,Kiran,40000,HrAdmin        
1205,Kranth,i30000,OpAdmin        


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

########## Loading Data From Local Drive to Hive Table
LOAD DATA LOCAL INPATH '/home/keanesoft/rahul_hadoop/tablex.txt' OVERWRITE INTO TABLE rahulnutakki.rahul_testx;
LOAD DATA LOCAL INPATH '/home/keanesoft/rahul_hadoop/tabley.txt' OVERWRITE INTO TABLE rahulnutakki.rahul_testy;

/home/keanesoft/rahul_hadoop






CREATE TABLE `rahul_test`(
  `eid` string, 
  `name` string, 
  `salary` string, 
  `designation` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/rahul_test'


-- Insert Syntax

insert overwrite table rahulnutakki.rahul_testx
select * from rahulnutakki.rahul_testy;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Alter Table --
--Below are the most common uses of the ALTER TABLE command:
--You can rename table and column of existing Hive tables.
--You can add new column to the table.
--Rename Hive table column.
--Add or drop table partition.
--Add Hadoop archive option to Hive table.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

ALTER TABLE [old_db_name.]old_table_name RENAME TO [new_db_name.]new_table_name;
ALTER TABLE name ADD COLUMNS (col_spec[, col_spec ...]);
ALTER TABLE name CHANGE column_name new_name new_type;
ALTER TABLE name REPLACE COLUMNS (col_spec[, col_spec ...]);
ALTER TABLE table_name ADD COLUMNS (column_defs);
ALTER TABLE table_name REPLACE COLUMNS (column_defs);
ALTER TABLE table_name CHANGE column_name new_name new_type;
ALTER TABLE table_name DROP column_name;
-- Switch a table from internal to external.
ALTER TABLE table_name SET TBLPROPERTIES('EXTERNAL'='TRUE');

-- Switch a table from external to internal.
ALTER TABLE table_name SET TBLPROPERTIES('EXTERNAL'='FALSE');

ALTER TABLE rahulnutakki.rahul_testy
ADD COLUMNS (deptid STRING);

ALTER TABLE rahulnutakki.rahul_testy DROP deptid;

INSERT INTO TABLE rahulnutakki.rahul_testy VALUES ('1206','new_emp','60000','OPSAdmin',null);


-- One method to delete a column is to replace columns with the one that is not needed --
ALTER TABLE rahulnutakki.rahul_testy REPLACE COLUMNS (eid string, name String, salary String, designation String);

ALTER TABLE rahulnutakki.rahul_test REPLACE COLUMNS (eid string empId string);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PARTITION and Bucketing Discussion and Practicals:



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SQOOP PRACTICE
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- To login to MySQL
mysql Login :
mysql -u retail_dba -h nn01.itversity.com -p
itversity
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Sqoop is a tool designed to transfer data between Hadoop and relational databases or mainframes. 
--You can use Sqoop to import data from a relational database management system (RDBMS) such as MySQL or
--Oracle or a mainframe into the Hadoop Distributed File System (HDFS), transform the data in Hadoop MapReduce, 
--and then export the data back into an RDBMS.

--Sqoop automates most of this process, relying on the database to describe the schema for the data to be imported.
--Sqoop uses MapReduce to import and export the data, which provides parallel operation as well as fault tolerance.
--With Sqoop, you can import data from a relational database system or a mainframe into HDFS. The input to the import
-- process is either database table or mainframe datasets. 
--For databases, Sqoop will read the table row-by-row into HDFS. For mainframe datasets, Sqoop will read records 
--from each mainframe dataset into HDFS.
--The output of this import
--process is a set of files containing a copy of the imported table or datasets. 
--The import process is performed in parallel. For this reason, the output will be in multiple files. 
--These files may be delimited text files (for example, with commas or tabs separating each field), 
--or binary Avro or SequenceFiles containing serialized record data.

--A by-product of the import process is a generated Java class which can encapsulate one row of the imported table. 
--This class is used during the import process by Sqoop itself. 
--The Java source code for this class is also provided to you, for use in subsequent MapReduce processing of the data. 
--This class can serialize and deserialize data to and from the SequenceFile format. 
--It can also parse the delimited-text form of a record. 
--These abilities allow you to quickly develop MapReduce applications that use the HDFS-stored records in your processing pipeline.
--You are also free to parse the delimiteds record data yourself, using any other tools you prefer.

--After manipulating the imported records (for example, with MapReduce or Hive) you may have a result data set which you can then export
--back to the relational database. 
--Sqoop’s export process will read a set of delimited text files from HDFS in parallel, parse them nto records,
--i and insert them as new rows in a target database table, for consumption by external applications or users.

--Sqoop includes some other commands which allow you to inspect the database you are working with. 
--For example, you can list the available database schemas (with the sqoop-list-databases tool) and tables within a schema 
--(with the sqoop-list-tables tool). Sqoop also includes a primitive SQL execution shell (the sqoop-eval tool).

--Most aspects of the import, code generation, and export processes can be customized.
--For databases, you can control the specific row range or columns imported. You can specify particular delimiters
--and escape characters for the file-based representation of the data, as well as the file format used. 
--You can also control the class or package names used in generated code. Subsequent sections of this document
--explain how to specify these and other arguments to Sqoop.

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

sqoop-version

Running Sqoop version: 1.4.6.2.6.5.0-292
Sqoop 1.4.6.2.6.5.0-292
git commit id 0933a7c336da72cabb1ddfa5662416a374521b67
Compiled by jenkins on Fri May 11 07:59:00 UTC 2018

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

codegen            Generate code to interact with database records
create-hive-table  Import a table definition into Hive
eval               Evaluate a SQL statement and display the results
export             Export an HDFS directory to a database table
help               List available commands
import             Import a table from a database to HDFS
import-all-tables  Import tables from a database to HDFS
import-mainframe   Import mainframe datasets to HDFS
list-databases     List available databases on a server
list-tables        List available tables in a database
version            Display version information

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



In SQL


show databases;

use databaseName;

show tables;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Sqoop Import Commands:
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
-username retail_user \
-password itversity \
-table departments \
-target-dir "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp1/"

--'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/rahul_test'


-- checking one of the part file created after the import
hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp1/part-m-00001"

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp1/part-m-00000"

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp1/part-m-00002"

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp1/part-m-00003"


-- USING WHERE CLAUSE AND MAPPERS
sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
-username retail_user \
-password itversity \
-table departments \
-where "department_id > 5" \
-target-dir "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp3/" \
-m 4  

hadoop fs -ls "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp3/" 

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp3/part-m-00000"

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp3/part-m-00001"

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
-username retail_user \
-password itversity \
-table departments \
-where "department_id > 5" \
-target-dir "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp4/" \
-m 1  

hadoop fs -ls "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp4/" 

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_tmp4/part-m-00000"

--mapper
-- using mappers reduces the execution time
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Free Form Query:
Talk About $CONDITIONS and split-by
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
sqoop import \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  -username retail_user \
  -password itversity \
  --query 'SELECT * from categories WHERE $CONDITIONS' \
  --split-by category_id \
  --target-dir "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query2/" \
  -m 10


hadoop fs -ls "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query2/"

hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query2/part-m-00000"
hadoop fs -cat "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query2/part-m-00003"

-- what is $ query
-- what is split by
-- what is a $ condition
-- when we give $conditions sqoop understands that it can split data


Free Form Query:
sqoop import \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  -username retail_user \
  -password itversity \
  --query 'SELECT * from categories WHERE $CONDITIONS' \
  --fields-terminated-by '|' \
  --split-by category_id \
  --target-dir "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query3/" \
  -m 10


Point hive table on top of Data that is imported from Sqoop:
--------------------------------------------------------------

drop table if exists srikanth_course.categories;
CREATE EXTERNAL Table IF NOT EXISTS srikanth_course.categories (
category_id string,
category_department_id string,
category_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query3/';




Importing Data directly into Hive:
======================================

Step1:

CREATE EXTERNAL Table IF NOT EXISTS rahulnutakki.categories_new (
category_id string,
category_department_id string,
category_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/sqoop_0709_categories_query3/';

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--table categories \
--target-dir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/categories_new6/ \
--hive-import \
--create-hive-table \
--hive-table rahulnutakki.categories_new

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SPARK PRACTICE
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/


hadoop fs -put /home/keanesoft/rahul_hadoop/sparktest1.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723

hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723

hdfs://nn01.itversity.com:8020/apps/rahul_hadoop/

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/rahul_hadoop


---- Data For the test file sparktest1 -----
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Hadoop
Spark
Spark
Spark
Spark
Spark
Spark
Spark
Spark
Spark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark
HadoopSpark


hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723/



--PySpark
cd rahul_hadoop
pyspark

textfile = sc.textFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723/")
textfile
type(textfile)

textfile = sc.textFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723/")
counts = textfile.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.take(10)

counts.getNumPartitions()

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--Create a Spark context.
sqlContext = SQLContext(sc)
--Create Spark Dataframe and preview. Below example uses mtcars data
sdf = sqlContext.createDataFrame(mtcars) 
sdf.printSchema()
--Display the content of dataframe by
sdf.show(5)
--You can select a column by
sdf.select(‘mpg’).show(5)
--Filter the column by , where ‘mpg’ is less then 18.
sdf.filter(sdf[‘mpg’] < 18).show(5)
--Operating on a column.
sdf.withColumn(‘wtTon’, sdf[‘wt’] * 0.45).show(6)
--Grouping and aggregation.
sdf.groupby([‘cyl’])\
.agg({“wt”: “AVG”})\
.show(5)
--Save Dataframe as a Temporary Table and sql query on the saved table
# Register this DataFrame as a table.
sdf.registerTempTable(“cars”)

# SQL statements can be run by using the sql method
highgearcars = sqlContext.sql(“SELECT gear FROM cars WHERE cyl >= 4 AND cyl <= 9”)
highgearcars.show(6)
---------------------------------------------------------------------------------------------------------------------------------------

sparktest2.txt

hadoop is fast
hive is sql on hdfs
spark is superfast
spark is awesome

hadoop fs -put /home/keanesoft/rahul_hadoop/sparktest2.txt hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723_2
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723_2

----- FOR MAP ---------
hdfs_path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723_2/"
data = sc.textFile(hdfs_path)
wc = data.map(lambda line:line.split(" "));
wc.collect()

temp_fm = 



data.max()
data.min()
data.mean()
data.stdev()


hdfs_path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723_2/"
data = sc.textFile(hdfs_path)
wc = data.map;
wc.collect()

---Apply a function to each RDD element---

hdfs_path = "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/spark_0723_2/"
data = sc.textFile(hdfs_path)
x_1 = data.map(lambda x: x+(x[1],x[0]));
x_1.collect()


------ For FlatMap --------
fm = data.flatMap(lambda line:line.split(" "));
fm.collect()
temp_fm = fm.map(lambda temp: (temp, 1))
temp_fm2 = temp_fm.reduceByKey(lambda a, b: a + b)


-- 1. parallelize
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
distData.getNumPartitions()
distData.take(10)
distData.collect(10)


distData.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/distData2")

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/distData2

hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/distData2/part-00001




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
   "word10",
   "word11",
   "word12",
   "word13",
   "word14",
   "new_word1",
   "new_word2",
   "new_word3",
   "new_word4",
   "new_word5",
   "new_word6",
   "new_word7",
   "new_word8",
   "new_word9",
   "new_word10"]
)

counts = words.count()
counts 

coll = words.collect()
coll

print('count of words: ', counts)

print(coll)


fore = words.foreach(f)  
def f(x):
print(x)

*************************************************************************************

--filterout words:
words_filter = words.filter(lambda x: 'word' in x)
filtered = words_filter.collect()
print(filtered)
type(filtered)
type(words_filter)

*************************************************************************************

------- example k,v pair: ---------
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
mapping


words_map = words.flatMap(lambda x: (x, 1))
mapping = words_map.collect()
mapping

*************************************************************************************

-------- example reduce function -----------
from operator import add
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print(adding)
type(nums)
type(adding)

*************************************************************************************

------ example --------

x = sc.parallelize([("spark", 1), ("hadoop", 4),("hadoop", 6)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print(final)

*************************************************************************************

# Working with Key-Value Pairs
#example:
k,v pair:
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print(mapping)

*************************************************************************************

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------ Example for Repartition and Colasce: ------------------ 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Keep in mind that repartitioning your data is a fairly expensive operation.
--Spark also has an optimized version of repartition() called coalesce() that allows avoiding data movement,
--but only if you are decreasing the number of RDD partitions.




repartition:
------------
data1 = [("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]
data2 = [("spark", 2), ("hadoop", 5),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]

rdd1 = sc.parallelize(data1)
rdd2 = sc.parallelize(data2)

-- inner join
joined_rdd = rdd1.join(rdd2)
-- left outer join
joined_rdd = rdd1.leftOuterJoin(rdd2)
-- cartesian product
joined_rdd = rdd1.cartesian(rdd2)

# joined_rdd.collect()

# rdd1.getNumPartitions()
# rdd2.getNumPartitions()

joined_rdd_rep = joined_rdd.repartition(2)
joined_rdd_rep.getNumPartitions()

joined_rdd_rep.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/joined_rdd_rep")

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/joined_rdd_rep/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/joined_rdd_rep/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/joined_rdd_rep/part-00001


--coalasce:--

data3 = [("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]
data4 = [("spark", 2), ("hadoop", 5),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6),("spark", 1), ("hadoop", 4),("hadoop", 6)]

rdd3 = sc.parallelize(data3)
rdd4 = sc.parallelize(data4)

joined_col_rdd = rdd3.join(rdd4)
joined_col_rdd.getNumPartitions()


tmp_rdd = joined_col_rdd.coalesce(2)
tmp_rdd.getNumPartitions()
tmp_rdd.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/rdd_col_col1")

hadoop fs -ls "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new"
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/rdd_col_col1
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/rdd_col_col1/part-00000


Repartition:
rdd_rep = rdd1.repartition(4)
rdd_rep.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/rdd_rep_new")

Coalesce:
rdd_col = rdd1.coalesce(4)
rdd_col.saveAsTextFile("hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/rdd_col_new")
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/rahulnutakki.db/pyspark_new/rdd_rep/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00001
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00002
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_rep/part-00003

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00000
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00001
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00002
hadoop fs -cat hdfs://nn01.itversity.com:8020/apps/hive/warehouse/srikanth_course.db/pyspark_new/rdd_col/part-00003

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------





















