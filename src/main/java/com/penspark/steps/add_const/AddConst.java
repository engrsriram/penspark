package com.penspark.steps.add_const;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;
import org.dom4j.Node;

import com.penspark.steps.StepInterface;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.upper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AddConst extends AddConstMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<String> oper =  new ArrayList<String>();
	//String[] oper;
	static Logger log = Logger.getLogger(AddConst.class);
	public AddConst(Element element) {
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
		
		// this.Output =s.selectExpr(Colname2); 
		
		List<Node> nodes2 = this.el.selectNodes("fields/field" );
		Iterator<Node> iter2=nodes2.iterator();
		while(iter2.hasNext()){
			Element element2=(Element)iter2.next();
			//this.Switchoptr.put(element2.elementText("target_step"), this.fieldfocus +" = '"+element2.elementText("value") +"'");
			//          <aggregate>count</aggregate>
	          //<subject>job_Title</subject>
	          //<type>COUNT_DISTINCT</type>
	          ///<valuefield/>
			//log.info(element2.asXML());
			
			log.info("name:" + element2.elementText("name"));
			log.info("type:" +element2.elementText("type"));
			log.info("nullif:" +element2.elementText("nullif"));
			
			s = s.withColumn(element2.elementText("name"), lit(element2.elementText("nullif")));
		}
		
		this.Output = s; 
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