package com.penspark.steps.set_field_value;

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
import static org.apache.spark.sql.functions.upper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SetFieldValue extends SetFieldValueMeta implements  StepInterface 
{
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	ArrayList<String> oper =  new ArrayList<String>();
	//String[] oper;
	static Logger log = Logger.getLogger(SetFieldValue.class);
	public SetFieldValue(Element element) {
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
		
		
		@SuppressWarnings("unchecked")
		List<Node> nodes2 = this.el.selectNodes("fields/field");
        Iterator<Node> iter2=nodes2.iterator();
        	while(iter2.hasNext()){
        		Element element2=(Element)iter2.next();
        	s = s.withColumn(element2.elementText("name"), col(element2.elementText("replaceby")));
        	}
		
		 this.Output =s; 
		 
	}
}