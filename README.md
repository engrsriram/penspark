# penspark
This JAVA project will read pentaho ETL Transformation and run as Apache.spark application


This application for those who are not interested in coding apache spark , then, you can use pentaho(drag / drop) GUI application , this application will tranlates your pentaho ETL logic into spark cluster application. 


## Little backgroud

for those who don't know about pentaho?
its an ETL tool , similar to infomatica etc
i am using 4.3 version for this development.[available in this link](https://github.com/pentaho/pentaho-kettle/tree/4.3)

### Application preparation:

	$git clone https://github.com/engrsriram/penspark
	mvn install
	mvn package
	cd target/
	java -cp lib/*;app-0.0.1-SNAPSHOT.jar com.penspark.App -file="E:\pentaho_code\main.ktr"
	
Assuming "E:\pentaho_code\main.ktr" is you pentaho ETL logic resides. 
