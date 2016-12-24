package com.penspark.steps.group_by;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

import java.util.ArrayList;
import java.util.Arrays;

public class GroupBy extends GroupByMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<String> oper =  new ArrayList<String>();
	//String[] oper;
	static Logger log = Logger.getLogger(GroupBy.class);
	public GroupBy(Element element) {
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
      Column[] Colname2 = this.operGroupBy.toArray(new Column[0]);
      Column[] Agg = this.operAgg.toArray(new Column[0]);
		log.info(">>>:"+Arrays.deepToString(Colname2));
		  
	//	String[] Colname2 = this.oper.toArray(new String[0]);
		log.info(">>>::"+Arrays.deepToString(Agg));
		// Dataset<Row> people_sorted1 = people.groupBy(er).agg(count(er).as("Count")); 
		 
		this.Output =s.groupBy(Colname2).agg(Agg[0]); 
		 
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