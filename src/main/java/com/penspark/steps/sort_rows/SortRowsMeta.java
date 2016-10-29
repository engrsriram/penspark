package com.penspark.steps.sort_rows;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.dom4j.Element;
import org.dom4j.Node;

import scala.collection.Seq;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class SortRowsMeta  {
	

	static Logger log = Logger.getLogger(SortRowsMeta.class);
	
	ArrayList<Column> operMaster =  new ArrayList<Column>();
	ArrayList<String> operend =  new ArrayList<String>();
	
	public SortRowsMeta(Element el)
	{
		

		@SuppressWarnings("unchecked")
		List<Node> nodes2 = el.selectNodes("fields/field");
        Iterator<Node> iter2=nodes2.iterator();
        	while(iter2.hasNext()){
        		Element element2=(Element)iter2.next();
        		System.out.println("Sort element:"+element2.asXML());
        		log.info("Name:"+ element2.elementText("name"));
        		log.info("Ascending:"+ element2.elementText("ascending"));
        		log.info("Case Sensitive:"+ element2.elementText("case_sensitive"));
        		Column er ;
        		if(element2.elementText("case_sensitive").equals("N")){
        			if(element2.elementText("ascending").equals("Y")){
            			
        			er = lower(col(element2.elementText("name"))).asc();
        			}else{
        				er = lower(col(element2.elementText("name"))).desc();
        			}
        				
        			//lower(col("job_title"));
        		}
        		else
        		{
        			if(element2.elementText("ascending").equals("Y")){
            			
        			er = col(element2.elementText("name")).asc();
        			}else{
        				er = col(element2.elementText("name")).desc();
        			}
        		}
        		this.operMaster.add(er);
        		//this.Switchoptr.put("type1", "Engr");
        	}
	}

	public ArrayList<Column> getalloperator() {
		// TODO Auto-generated method stub
		return this.operMaster;
	}
	
}