# penspark
This JAVA application help you scalling your existing pentaho ETL Transformation with the power of Apache Spark.


This application for those who prefer GUI application instead of coding apache spark.

You can make ETL logic in pentaho(drag / drop) GUI application , this application will tranlates your pentaho ETL logic into spark cluster application at runtime. 


## Little backgroud

for those who don't know about pentaho?
its an ETL tool , similar to infomatica
i am using 4.3 version for this development.[available in this link](https://github.com/pentaho/pentaho-kettle/tree/4.3)

### Application preparation:

	$git clone https://github.com/engrsriram/penspark
	mvn install
	mvn package
	cd target/
	java -cp lib/*;app-0.0.1-SNAPSHOT.jar com.penspark.App -file="E:\pentaho_code\main.ktr"
	
Assuming "E:\pentaho_code\main.ktr" is your pentaho ETL logic resides. 
