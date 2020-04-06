package com.SparkJavaUDF.SHAUDF;

//Import Spark UDF
import org.apache.spark.sql.api.java.UDF1;

//Import MessageDigests for SHA algorithm
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets; 

//implement Spark UDF... 
//first parameter is the arg type and second is return type
//for more parameters use UDF2/UDF3..... etc...

public class getSHA  implements UDF1 <String, String>{

	@Override
	public String call(String plainText ) throws Exception {
		
		// SHA hashing for an example
		// Any custom logic can be built in this call method 
		StringBuilder hexString = new StringBuilder();
		try {
			
			MessageDigest sha = MessageDigest.getInstance("SHA-256");
			
			byte[] shahash = sha.digest(plainText.getBytes(StandardCharsets.UTF_8));
			
			for (int i = 0; i < 32; i++) 
			{
			    	String hex = Integer.toHexString(0xff & shahash[i]);
			    	if(hex.length() == 1) hexString.append('0');
			    	hexString.append(hex);
		    }
		}
		catch (Exception ex) {
			return ex.toString();
		}
		return hexString.toString();  
 }
}
