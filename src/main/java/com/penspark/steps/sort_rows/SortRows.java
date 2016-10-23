package com.penspark.steps.sort_rows;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;

import scala.collection.Seq;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

import java.util.ArrayList;
import java.util.Arrays;

public class SortRows extends SortRowsMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<Column> oper =  new ArrayList<Column>();
	//String[] oper;
	static Logger log = Logger.getLogger(SortRows.class);
	public SortRows(Element element) {
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
	public void workout(Dataset<Row> s, SparkSession spark) {
		//String operations.
		//s.createOrReplaceTempView("StringOperatingtable");
		// Column n = upper(s.col("Name"));
		s.sort("age > 20", "asdf < 32");
		Column d = col("age > 20");
		 
		// Seq<Column> sortExprs = super.getSortOptr();
		this.oper = super.getalloperator();
		//this.oper.add(col("age > 20"));
		//this.oper.add(col("Apple"));
		
        Column[] Colname2 = this.oper.toArray(new Column[0]);
		log.info(">>>:"+Arrays.deepToString(Colname2));
		// String[] Colname1 = {"upper(Name) as Name" , "Class" , "upper(Dorm) as Dorm" , "upper(Room) as Room" , "GPA"};
		//sortExprs.`
		 this.Output =s.sort(Colname2); 
		 
	}
}