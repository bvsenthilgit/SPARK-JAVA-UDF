# SPARK-JAVA-UDF

## Create a SPARK UDF in JAVA and invoke in PYSPARK

 
Hi! I ran into a situation where I had to use a custom Java built function in the PySpark. The Java Jar is common component used in multiple applications and I do not want to replicate it in Python to avoid redundancy & maintenance issues later in time.

Here is a solution to invoke the Java component in PySpark !

There are two different way to invoke Java function in PySpark by spinning a JVM:

1.  Invoke JVM using Spark Context as below, but in our case we need to apply the Java function as a UDF and spark context jvm will not be available inside the spark session, pushing us to use the next option. sc._jvm.< java package >.< java class>.< method>()

3.  Implement the SPARK UDF interface in Java and register the Java udf in PySpark. This allows us to use the UDF in PySpark as below:

# UDF Details

## Step 1: 
Git repo shows the implementation of Spark Interface. 
``` 

import org.apache.spark.sql.api.java.UDF1;  
public class getSHA implements UDF1 <String, String>{  	 		
	@Override  	 	
	public String call(String plainText ) throws Exception {  				
		< Java Custom function >
	}   	
}
```
Based on number of arguments in java use UDF2, UDF3, UDF4…. interface from Spark UDF 
## Step 2: 
Register the UDF in Spark Session 
```
#Register the Java UDF in SPARK Session
spark.udf.registerJavaFunction("udfGetSHAhash", "com.SparkJavaUDF.SHAUDF.getSHA")
```
## Step 3: 
Use the UDF in SQL context as below: 
```
sql_encrypt= "SELECT CustomerNumber, CustomerName, CustomerType, CustomerSecret , udfGetSHAhash(CustomerSecret) as SHA from tempTable"

df1=spark.sql(sql_encrypt)
```

## Step 4: 
While executing the Spark code, make sure to include the Jars as below:
```
#Command to run the spark code 
export path="/SHAUDF-0.0.1-SNAPSHOT.jar,/teradata-connector-1.6.0.jar,/terajdbc4.jar,/tdgssconfig.jar"

/usr/bin/spark-submit --jars $path /PYSPARK_UDF_DEMO.py
```
PySpark code (PYSPARK_UDF_DEMO.py) also include the git repo

Thanks! 
[@bvsenthil](https://www.linkedin.com/in/bvsenthil/)
