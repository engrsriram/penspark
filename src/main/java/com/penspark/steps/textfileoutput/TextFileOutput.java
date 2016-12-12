package com.penspark.steps.textfileoutput;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;

import com.penspark.steps.StepInterface;

public class TextFileOutput extends TextFileOutputMeta  implements  StepInterface
{
	static Logger log = Logger.getLogger(TextFileOutput.class);

	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	public TextFileOutput(Element element) {
		super(element);
	}

	@Override
	public void workout(SparkSession spark, Dataset<Row> s) {
		log.info("TextFileOutput --Started working now");
		 //Flat file output. 
		
       // s.foreach((ForeachFunction<Row>) person -> {System.out.print(person.get(person.fieldIndex("Name")));System.out.println(person.get(1));});
        log.info("Filename" + this.filename);
        log.info("Data before writing into file");
        s.show();
        //.save(this.filename);
        
        
        /*
        s.coalesce(1).select("Name , Class, Dorm , Room, GPA").write()
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(this.filename);
        */
        //String[] er = {"Name" , "Class", "Dorm" , "Room", "GPA"};
        String[] er = this.Collist;
        log.info("output file list" + er);
        s.coalesce(1).selectExpr(this.Collist).write()
        .format("com.databricks.spark.csv")
        .option("header", "true").option("dateFormat", "MM/dd/yyyy")
        .save(this.filename);
//        .save("E:\\Datasets\\out\\file.txt");
        
        s.printSchema();
        
		
	}

	@Override
	public void workout(SparkSession spark, Dataset<Row> s, Dataset<Row> l) {
    	 
	}
	
	@Override
	public String getString() {
		
		return "Get-ST-TextFileOutput";
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
}
