#####################################################################################################################
#PYSPARK program
#register the Java UDF 
#Load the data into data frame
#use the UDF in SQL Context as required
#####################################################################################################################


#Import required libraries 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark import SparkContext
import teradata
from datetime import datetime

sc = SparkContext()

#Adding Jars to the spark execution
udfpath="<localpath>/SHAUDF-0.0.1-SNAPSHOT.jar"
teradatadriver="<jarpath>/lib/teradata-connector-1.6.0.jar, <jarpath>/lib/terajdbc4.jar, <jarpath>/lib/tdgssconfig.jar"
path=teradatadriver+", "+udfpath

#Spark Session
spark = SparkSession.builder.appName("sprkUDFexample").config("spark.driver.extraClassPath", path).getOrCreate()

#Register the Java UDF in SPARK Session
spark.udf.registerJavaFunction("udfGetSHAhash", "com.SparkJavaUDF.SHAUDF.getSHA")


#ODBC connection details
driver = 'com.teradata.jdbc.TeraDriver'
sql = "SELECT CustomerNumber, CustomerName, CustomerType, CustomerSecret FROM UDFTEST "
url = "jdbc:teradata://hostname/Database=schemaname"
user = "dbuser"
password = "dbpassword"


#ODBC connection to Teradata or any data source
df=spark.read \
        .format('jdbc') \
        .option('driver', driver) \
        .option('url', url) \
        .option('dbtable', '({sql}) as src'.format(sql=sql)) \
        .option('user', user) \
        .option('password', password) \
        .load()
df.show(3)

df.registerTempTable("tempTable")

sql_encrypt= "SELECT CustomerNumber, CustomerName, CustomerType, CustomerSecret , udfGetSHAhash(CustomerSecret) as SHA from tempTable" 
df1=spark.sql(sql_encrypt)

df1.show(3)

#Command to run the spark code
#export path="<jarpath>/SHAUDF-0.0.1-SNAPSHOT.jar,<jarpath>/teradata-connector-1.6.0.jar,<jarpath>/terajdbc4.jar,<jarpath>/tdgssconfig.jar"
#/usr/bin/spark-submit --jars  $path /home/sbogana/PYSPARK_UDF_DEMO.py