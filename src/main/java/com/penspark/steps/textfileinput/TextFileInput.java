package com.penspark.steps.textfileinput;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Element;
import com.penspark.steps.StepInterface;

public class TextFileInput extends TextFileInputMeta implements  StepInterface
{
	static Logger log = Logger.getLogger(TextFileInput.class);
	Dataset<Row> Input = null;
	Dataset<Row> Output = null;
	File[] filenm;
	public TextFileInput(Element element) {

		super(element);
		this.filenm = super.getfilenames();
	}


	@Override
	public void workout(Dataset<Row> s, SparkSession spark) {
		
		/*log.info("Gather all the details about the Source File or multiple files");
		log.info("Declare all the variable considerations , encloseres , separaters. etc");
		log.info("open loop and read all the data , it error then though IO error");
		log.info("If Everything is fine, then prepare Spark Data set");
		log.info("Do inital data conversions, Date formats, Data type managements etc");
		log.info("Return back to the results");
		
		
		Algorithm:
		----------
		
		if s is null then just read all the input from file and create DataSet to return it. 
					// Apply a schema to an RDD of JavaBeans to get a DataFrame
				Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
		
	
		
	
		StructType customSchema = new StructType(new StructField[] {
		    new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("Class", DataTypes.IntegerType, true, Metadata.empty()),
		    new StructField("Dorm", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("Room", DataTypes.StringType, true, Metadata.empty()),
		    new StructField("GPA", DataTypes.FloatType, true, Metadata.empty())
		});
	*/
		try {
			//log.info("This is Schema :"+ this.Schemafinal.toString()) ;
			/*this.Output = spark.read()
				    .format("com.databricks.spark.csv")
				    .schema(this.Schemafinal)
				    .option("header", "true")
				    .load("E:\\Datasets\\in\\file.csv");*/
			
	
			this.Output =  spark.read()
				    .format("com.databricks.spark.csv")
				    .schema(this.Schemafinal)    
				    .option("header", "true")
				    .load(this.filenm[0].getAbsolutePath());
				log.info("Textfileinput is working now"); 

		} catch (Exception e) {
			log.error("Schemafinal - bracked"+ e.getMessage());
		}
		
	//	this.Output.show();
	}


	@Override
	public String getString() {
		return "Get-ST-TextFileInput";
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
