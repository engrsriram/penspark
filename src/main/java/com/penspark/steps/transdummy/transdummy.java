package com.penspark.steps.transdummy;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;

public class transdummy implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	static Logger log = Logger.getLogger(transdummy.class);
	public transdummy(Element element) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void workout(SparkSession spark, Dataset<Row> s) {
		log.info("This is dummy Step , so i don't do anything");
		this.Output = s;
		//return this.Output;
	}

	@Override
	public void workout(SparkSession spark, Dataset<Row> s, Dataset<Row> l) {
    	 
	}
	@Override
	public String getString() {
		return "Get-ST-String Operation";
	}

	@Override
	public Dataset<Row> getOutput(String name) {
		// TODO Auto-generated method stub
		return this.Output;
	}

	
	@Override
	public void Setinput(Dataset<Row> s) {
		// TODO Auto-generated method stub
		this.Input = s;
		
	}

	@Override
	public void setfrom(String N) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getfrom() {
		// TODO Auto-generated method stub
		return null;
	}
}