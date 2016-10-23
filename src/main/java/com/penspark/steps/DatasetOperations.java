package com.penspark.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.mortbay.log.Log;


public class DatasetOperations {
	static Logger log = Logger.getLogger(DatasetOperations.class);
	Dataset<Row> Result = null;

	// get two dataset into single Dataset
	public static Dataset<Row> combineset(Dataset<Row> a , Dataset<Row> b)
	{
		Dataset<Row> t = null;
		log.info("going for union operation");
		if(a !=null && b != null){
			log.info("As both data set are not null , making union of the data");
			 t = a.union(b); 
		}
		else if (a != null)
		{
			log.info(" only a is valid");
			t = a;
		}
		else if(b != null)
		{
			log.info("only b is valid");
			t = b;
		}
		log.info("Union Operation Completed");
		return t;
	}
	

}