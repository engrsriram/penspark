package com.penspark.steps.switch_case;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;

public class SwitchCase extends SwitchCaseMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	ArrayList<Dataset<Row>> Output = new ArrayList<Dataset<Row>>();
	String SqlString = "";
	static Logger log = Logger.getLogger(SwitchCase.class);
	SwitchCase(Element element) {
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
		int index = this.Destinations.indexOf(name);
		return this.Output.get(index);
	}

	@Override
	public void Setinput(Dataset<Row> s) {
		// TODO Auto-generated method stub
		this.Input = s;
		
	}

	@Override
	public void workout(Dataset<Row> s, SparkSession spark) {
		 s.createOrReplaceTempView("ComOperation");
        for(String Opr : this.Operatators){
        	this.Output.add(s.filter(Opr));
        }
		 
	}
}