package com.penspark.steps.commonoperations;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;

public class CommonOperations extends CommonOperationsMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	String SqlString = "";
	static Logger log = Logger.getLogger(CommonOperations.class);
	CommonOperations(Element element) {
		super(element);
		// TODO Auto-generated constructor stub
		
		// collect an xml into SQL
		
		this.SqlString = "";
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
	public void workout(SparkSession spark, Dataset<Row> s) {
		 s.createOrReplaceTempView("ComOperation");
        this.Output = spark.sql(this.SqlOpr);
	}
	@Override
	public void workout(SparkSession spark, Dataset<Row> s, Dataset<Row> l) {
    	 
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