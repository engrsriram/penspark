package com.penspark.steps;


import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

/*
 * Todo:
 * Read all the details in given trans file and make it as a meta data object for further processing
 */
public interface  StepInterface {
	
	int No_inHops = 0;
	int No_OutHops = 0;
	Boolean Distribute = false;
	public Dataset<Row> Input = null;
	public Dataset<Row> Output = null;
	
	
	Map<String, String> In_Column_List = null ;
	Map<String, String> Out_Column_List = null ;
	
	public void workout(Dataset<Row> s, SparkSession spark);
	//public Dataset<Row> getOutput();
	public void Setinput(Dataset<Row> s);

	public String getString();
	public Dataset<Row> getOutput(String name);
}
