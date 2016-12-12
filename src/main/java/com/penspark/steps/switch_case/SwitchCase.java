package com.penspark.steps.switch_case;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;
import org.apache.spark.api.java.function.*;
import com.penspark.steps.StepInterface;

public class SwitchCase extends SwitchCaseMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	ArrayList<Dataset<Row>> Output = new ArrayList<Dataset<Row>>();
	Dataset<Row> DefaultOutput = null;
	//String SqlString = "";
	static Logger log = Logger.getLogger(SwitchCase.class);
	public SwitchCase(Element element) {
		super(element);
		//this.SqlString = "";
	}



	@Override
	public String getString() {
		return "Get-ST-String Operation";
	}

	@Override
	public Dataset<Row> getOutput(String name) {
		int index = this.Destinations.indexOf(name);
		if(index >= 0){
			//log.info(this.Output.get(index).count());
			return this.Output.get(index);
		}
		else {
			log.info(this.DefaultOutput.count());
			return this.DefaultOutput;
		}
	}

	@Override
	public void Setinput(Dataset<Row> s) {
		// TODO Auto-generated method stub
		this.Input = s;
		
	}

	@Override
	public void workout(SparkSession spark, Dataset<Row> s) {
		// s.createOrReplaceTempView("ComOperation");
		log.info("Working out of Switch case");
		
        Dataset<Row> buff = null;
        this.DefaultOutput = s;
		
        for(String Opr : this.Operatators){
        	//log.info(Opr);
        	//buff = s.filter("job_Title == 'Engineer'");
        	//
        	buff = s.filter(Opr);
			log.info("getting count of dataset: "+ Opr +" :"+buff.count());
        	this.Output.add(buff);
			this.DefaultOutput = this.DefaultOutput.except(buff);
			
        }	
		 
	}
	
	@Override
	public void workout(SparkSession spark, Dataset<Row> s, Dataset<Row> l) {
    	 
	}
}