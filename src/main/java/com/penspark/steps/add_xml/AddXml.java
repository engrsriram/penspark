package com.penspark.steps.add_xml;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

import java.util.ArrayList;
import java.util.Arrays;

public class AddXml extends AddXmlMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<String> oper =  new ArrayList<String>();
	//String[] oper;
	static Logger log = Logger.getLogger(AddXml.class);
	public AddXml(Element element) {
		super(element);
		
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
		//String operations.
		//s.createOrReplaceTempView("StringOperatingtable");
		// Column n = upper(s.col("Name"));
		 String[] Colname = s.columns();
		 this.oper = super.getalloperator(Colname);
		  
		String[] Colname2 = this.oper.toArray(new String[0]);
		log.info(">>>:"+Arrays.deepToString(Colname2));
		// String[] Colname1 = {"upper(Name) as Name" , "Class" , "upper(Dorm) as Dorm" , "upper(Room) as Room" , "GPA"}; 
		 this.Output =s.selectExpr(Colname2); 
		 
	}
	
	@Override
	public void workout(SparkSession spark, Dataset<Row> s, Dataset<Row> l) {
    	 
	}
}