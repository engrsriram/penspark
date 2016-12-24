package com.penspark.steps.filter_rows;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;
 public class FilterRows extends FilterRowsMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output_true = null;
	Dataset<Row> Output_false = null;
	String SqlString = "";
	static Logger log = Logger.getLogger(FilterRows.class);
	public FilterRows(Element element) {
		super(element);
		this.Filter_Condition = super.getFltrCondition();
	}
	@Override
	public String getString() {
		return "Get-ST-String Operation";
	}
	@Override
	public Dataset<Row> getOutput(String name) {
		if(this.option.equals(name)){
			return this.Output_false;
		}
		else{
			return this.Output_true;	
		}
		
	}
	@Override
	public void Setinput(Dataset<Row> s) {
		this.Input = s;
	}   
	@Override
	public void workout(SparkSession spark, Dataset<Row> s) {
		 //s.createOrReplaceTempView("ComOperation");
		//this.Filter_Condition
		   //this.Output_true = s.filter("job_Title like '%Engineer%'");
		this.Output_true = s.filter(this.Filter_Condition);
		   
		   if(!this.option.isEmpty()){
			   this.Output_false = s.except(this.Output_true);
			}
		   
		 
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