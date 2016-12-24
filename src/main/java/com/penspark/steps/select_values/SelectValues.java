package com.penspark.steps.select_values;

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

public class SelectValues extends SelectValuesMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<Column> oper =  new ArrayList<Column>();
	ArrayList<Column> Metaoper =  new ArrayList<Column>();
	//private Element ele;
	//String[] oper;
	static Logger log = Logger.getLogger(SelectValues.class);
	public SelectValues(Element element) {
		super(element);
		//this.ele = element;
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
		 String[] Colname = s.columns();
		 this.oper = super.getalloper(Colname);
		  
		 Column[] Colname2 = this.oper.toArray(new Column[0]);
		 String[] rmcolmn = this.operremove.toArray(new String[0]);
		log.info(">>>:"+Arrays.deepToString(Colname2)); 
		 this.Output =s.select(Colname2).drop(rmcolmn); 
		 
		 
		 String[] UpdatedColmn = this.Output.columns();
		 log.info(">>> Updated:"+Arrays.deepToString(UpdatedColmn));
		 Column[] MetaColum = super.getallMetaoper(UpdatedColmn).toArray(new Column[0]);
		 log.info(">>> Updated.MetaColum:"+Arrays.deepToString(MetaColum));
		 this.Output =this.Output.select(MetaColum); 
		 
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