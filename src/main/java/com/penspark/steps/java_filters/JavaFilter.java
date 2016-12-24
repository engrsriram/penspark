package com.penspark.steps.java_filters;

import static org.apache.spark.sql.functions.col;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;

public class JavaFilter extends JavaFilterMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output_true = null;
	Dataset<Row> Output_false = null;
	String SqlString = "";
	static Logger log = Logger.getLogger(JavaFilter.class);
	JavaFilter(Element element) {
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
		if(this.option){
			return this.Output_true;	
		}
		else{
			return this.Output_false;
		}
		
	}

	@Override
	public void Setinput(Dataset<Row> s) {
		// TODO Auto-generated method stub
		this.Input = s;
		
	}

	@Override
	public void workout(SparkSession spark, Dataset<Row> s) {
		 //s.createOrReplaceTempView("ComOperation");
		 this.Output_true = s.filter(this.Filter_Condition);
		 this.Output_false = s.except(this.Output_true);
        
        
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