package com.penspark.steps.textfileoutput;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

public class TextFileOutputMeta 
{
	static Logger log = Logger.getLogger(TextFileOutputMeta.class);
	String[] Collist;
	ArrayList<String> Collistmeta =  new ArrayList<String>();
	String filename;
	//String Filename;
	public TextFileOutputMeta(Element element) {
		@SuppressWarnings("unchecked")
		List<Node> nodes3 = element.selectNodes("type");
		@SuppressWarnings("unchecked")
		List<Node> nodes4 = element.selectNodes("file");
		Iterator<Node> filename=nodes4.iterator();
		while(filename.hasNext())
    	{
    		Element element2=(Element)filename.next();
    		String n = element2.elementText("name");
        	this.filename = new String(n);
    	}
		for(Node W: nodes3)
		{
			//.selectNodes("fields/field")
			log.info("Verify Type" + W.getStringValue());			
		}
 		log.info("Getting Name:" + this.filename.toString());
		@SuppressWarnings("unchecked")
		List<Node> nodes2 = element.selectNodes("fields/field");
        Iterator<Node> iter2=nodes2.iterator();
        	while(iter2.hasNext()){
        		
        		/*
        		 * Format
        		 * Length
        		 * Precision
        		 * Currency
        		 * Decimal
        		 * Group 
        		 * Trim Type
        		 * Null
        		 */
			
        		Element element2=(Element)iter2.next();
        		//System.out.println("String operation:"+element2.asXML());
        		
        		String result = (element2.elementText("name") != null)?element2.elementText("name") : "";
        		String n = result.toString();
    			
        		log.info("Result:" + result + ", n:" + n);
        		log.info("trim_type" + element2.elementText("trim_type"));
 				
        		if(!element2.elementText("type").isEmpty()){
        			//result = "LTRIM ( " + result + " )";
 				}
        		
        		if(!element2.elementText("trim_type").isEmpty()){
 					switch (element2.elementText("trim_type")) {
					case "left":
						result = "LTRIM ( " + result + " )";
						break;
					case "right":
						result = "RTRIM ( " + result + " )";
						break;
					case "both":
						result = "TRIM ( " + result + " )";
						break;

					default:
						break;
					}
				}
 			
				log.info("decimal" + element2.elementText("decimal"));
				if(!element2.elementText("decimal").isEmpty()){
					
				}
				
				log.info("Generated Result:" + result);
        		this.Collistmeta.add(result);
        	}
        	
        	this.Collist = this.Collistmeta.toArray(new String[0]);
	}

	
	
}