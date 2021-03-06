package com.penspark.steps.joins.stream_lookup;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

import java.util.ArrayList;
import java.util.Arrays;

public class StreamLookUp extends StreamLookUpMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<String> oper =  new ArrayList<String>();
	//String[] oper;
	static Logger log = Logger.getLogger(StreamLookUp.class);
	public StreamLookUp(Element element) {
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
	public void workout(SparkSession spark, Dataset<Row> s, Dataset<Row> l) {
		//String operations.
		//s.createOrReplaceTempView("StringOperatingtable");
		// Column n = upper(s.col("Name"));
		 String[] Colname = s.columns();
		 this.oper = super.getSelectItem(Colname);
		  
		String[] Colname2 = this.oper.toArray(new String[0]);
		log.info("()>>:"+Arrays.deepToString(Colname2));
		// String[] Colname1 = {"upper(Name) as Name" , "Class" , "upper(Dorm) as Dorm" , "upper(Room) as Room" , "GPA"}; 
		//this.Output =s.selectExpr(Colname2);
		//JoinType d "left_outer";
		//Dataset<Row> right = null;
		//Dataset<Row> tempTable = s.join(right,s.col("Jobname").equalTo(right.col("Job_name")) , "left_outer");
		Dataset<Row> tempTable = s.join(l,getConditionOptr() , "left_outer");
		this.Output = tempTable.selectExpr(Colname2).cache();
		 
	}
	
	@Override
	public void workout(SparkSession spark, Dataset<Row> s) {
    	 
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