package com.penspark;

import java.io.File;
import org.dom4j.*;
import org.dom4j.io.*;

import org.apache.log4j.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
 
import com.penspark.inits.Trans;
import com.penspark.players.Player;


public class App {
	static Logger log = Logger.getLogger(App.class);

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		CmdLine CMD = new CmdLine(args);
		String  file = CMD.getFilename();
		//String Filename = ;
		log.info("Recived File : " + file);
		try {
		    File fXmlFile = new File(file);
		    SAXReader reader = new SAXReader();
	        Document document = reader.read(fXmlFile);

	        Element classElement = document.getRootElement();

		    Trans T = new Trans(classElement);
		    //Trans T = new Trans();
			
		   // T.showStep();
			//if(T.valid()){
		    if(true){
				Player pilot = T.player;
				pilot.managehopes();
		        SparkConf conf = new SparkConf().setMaster("local").setAppName("Example App");

		        JavaSparkContext sc = new JavaSparkContext(conf);
		        @SuppressWarnings("deprecation")
				SQLContext sqlContext = new SQLContext(sc);
		 
		       SparkSession spark = SparkSession
		                .builder()
		                .appName("Pentaho Logic as Spark")
		                .config("spark.some.config.option", "some-value")
		                .config("spark.sql.warehouse.dir", "file:///C:/tmp/") 
		                .getOrCreate();
				pilot.play(spark);
			}

		} catch (Exception e) {
			log.error("ERROR:" +  e.getMessage());
		}
		
		/*
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL Example")
				.config("spark.some.config.option", "some-value").config("spark.sql.warehouse.dir", "file:///C:/tmp/")
				.getOrCreate();
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x) {
				return x * x;
			}
		});
		System.out.println(StringUtils.join(result.collect(), ","));

		// file input
		Dataset<?> df = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").load("E:\\Datasets\\in\\file.csv");

		// df
		// df.col("Name").toString().toUpperCase();
		// df.select("Name", "GPA").save("namesAndAges.parquet", "parquet");
		df.createOrReplaceTempView("people");

		//

		// String operations.
		Dataset<Row> sqlDF = spark.sql("SELECT upper(Name) as Name , Class FROM people");
		// sqlDF.show();

		sqlDF.select("Name", "Class").write().format("com.databricks.spark.csv").option("header", "true")
				.save("E://Datasets//in//outfile//file.csv");

		// Flat file output.
		sqlDF.foreach((ForeachFunction<Row>) person -> {
			System.out.print(person.get(person.fieldIndex("Name")));
			System.out.println(person.get(1));
		});

		log.info("Validate all the argument and deside log level of the application");
		log.info("Validate .ktr xml details");
		log.info("load Variable substitutions list");
		log.info("Validate all the resource availablility ");
		log.info("validate COLUMN flow though job");
		log.info("Order Steps and into iterateable list");
		*/
	}

	

}
