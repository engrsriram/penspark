package com.penspark.steps.textfileinput;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dom4j.Element;
import org.dom4j.Node;

public class TextFileInputMeta 
{
	static Logger log = Logger.getLogger(TextFileInputMeta.class);
	ArrayList<File> filename = new ArrayList<File>();
	StructType Schemafinal = new StructType();
	
	String includeHeader = "";
	Map<String, String> options = new HashMap<String, String>();
	
	StructField [] customSchema;
	LinkedList<StructField> obj = new LinkedList<StructField>();
	//List<StructField> asdf = new  List<StructField>();
	TextFileInputMeta(Element element)
	{
				@SuppressWarnings("unchecked")
				List<Node> nodes3 = element.selectNodes("type");
				
				// find the separator , 
				this.options.put("delimiter",element.selectSingleNode("separator").getText());

				// find header position, 
			    if(element.selectSingleNode("header").getText().equals("Y")){
					this.options.put("header","true");
			    	
			    }
			    if(!(element.selectSingleNode("enclosure").getText().isEmpty())){
					this.options.put("quote",element.selectSingleNode("enclosure").getText());
			    	
			    }
			    // date format:
				this.options.put("dateFormat", "MM/DD/YYYY");
			    if(!(element.selectSingleNode("include_field").getText().isEmpty())){
					this.includeHeader = element.selectSingleNode("include_field").getText();
			    	// get filename and put it into one of the column
			    }
				//  filename include or not. 
				
				
				
				@SuppressWarnings("unchecked")
				List<Node> nodes4 = element.selectNodes("file");
				Iterator<Node> filename=nodes4.iterator();
	        	while(filename.hasNext())
	        	{
	        		Element element2=(Element)filename.next();
	        		String n = element2.elementText("name");
	        		if(element2.elementText("filemask").isEmpty()){
			        	this.filename.add(new File(n));
	        			
	        		}
	        		else{
	        			File dir = new File(element2.elementText("name"));
	            		File [] files = dir.listFiles(new FilenameFilter() {
	            		    @Override
	            		    public boolean accept(File dir, String name) {
	            		        return name.matches(element2.elementText("filemask"));
	            		    }
	            		});

	            		for (File csvFile : files) {
	            		    //System.out.println("HollyGrail: "+csvFile);
	            		    this.filename.add(csvFile);
	            		}
			        	//this.filename.add(new File(n + element2.elementText("filemask")));
	        			
	        		}
	        	}
				for(Node W: nodes3)
				{
					log.info("Verify Type" + W.getStringValue());			
				}
				@SuppressWarnings("unchecked")
				List<Node> nodes2 = element.selectNodes("fields/field");
		        Iterator<Node>iter2=nodes2.iterator();
		        	while(iter2.hasNext()){
		        		Element element2=(Element)iter2.next();
		        		String n = element2.elementText("name");
		        		DataType a  = getDataTypes(element2.elementText("type"));
		        		log.info(a instanceof DataType);
		        		StructField as = new StructField(n, a, true, Metadata.empty());
		        		try{
		        			this.obj.add(as);
		        			log.info("Schemafinal addition :-)");
						} 
		        		catch (Exception e) {
							log.info("Failed in Schemafinal addition :-("+e.getMessage());
						}
		        	}
		        	
		        	try 
	        		{
		        		this.customSchema = this.obj.toArray(new StructField[this.obj.size()]);
		        		this.Schemafinal = new StructType(this.customSchema);
	        			log.info("Schemafinal addition :-)");
					} 
	        		catch (Exception e) {
						
						log.info("Failed in Schemafinal addition :-("+e.getMessage());
					}
	        	
	}


	private DataType getDataTypes(String elementText) {

		//String
		//Number
		//Date
		//Boolean
		//Integer
		//BigNumber
		//Serializable
		//Binary
		///log.info(">>>>>>"+ elementText);
		if(elementText.equals("String")){
			return DataTypes.StringType;
		}
		else if(elementText.equals("Integer"))
		{
			return DataTypes.IntegerType;
		}
		else if(elementText.equals("Number"))
		{
			return DataTypes.FloatType;
		}
		else if(elementText.equals("Date"))
		{
			return DataTypes.DateType;
		}else if(elementText.equals("Boolean"))
		{
			return DataTypes.BooleanType;
		}else if(elementText.equals("BigNumber"))
		{
			return DataTypes.DoubleType;
		}
		else
		{	//Serializable
			//Binary
			return DataTypes.BinaryType;
		}
			
			
	}


	public File[] getfilenames() {
		// TODO Auto-generated method stub
		return  this.filename.toArray(new File[this.filename.size()]);
	}


	
	
	
}