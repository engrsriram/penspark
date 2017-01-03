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
		
		
	}

	

}
