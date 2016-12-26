package com.penspark.steps.stream_lookup;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.dom4j.Element;
import org.dom4j.Node;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class StreamLookUpMeta  {
	static Logger log = Logger.getLogger(StreamLookUpMeta.class);
	
	Map<String, String> ConditionOptr = new HashMap<String, String>();
	Map<String, String> SelectItem = new HashMap<String, String>();
	String fromStep1;
	public StreamLookUpMeta(Element element) {
		this.fromStep1 = element.elementText("from");
		@SuppressWarnings("unchecked")
		List<Node> lookupkey = element.selectNodes("lookup/key" );
		Iterator<Node> iter1=lookupkey.iterator();
		while(iter1.hasNext()){
			Element element2=(Element)iter1.next();
			this.ConditionOptr.put(element2.elementText("name"), element2.elementText("field") );///+"'");
		}
		
		@SuppressWarnings("unchecked")
		List<Node> lookupvalue = element.selectNodes("lookup/value" );
		Iterator<Node> iter11= lookupvalue.iterator();
		while(iter11.hasNext()){
			Element element2=(Element)iter11.next();
			this.SelectItem.put(element2.elementText("name"), element2.elementText("rename") );///+"'");
		}
		
		
		
	}
	

	public String getConditionOptr()  {
		//String[] ActualCol
		String n = new String();
		//return this.ConditionOptr;
		Iterator<?> it = this.ConditionOptr.entrySet().iterator();
	    while (it.hasNext()) {
	        @SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)it.next();
	       // System.out.println(pair.getKey() + " : " + pair.getValue());
	        if(n.isEmpty())
	        	{
	        	//n = col(pair.getKey().toString()).equalTo(col(pair.getValue().toString()));
	        	 n = "L."+pair.getKey().toString() + " = " + "R."+ pair.getValue().toString() ; 
	        	}
	        else
	        	{
	        	//n = col(pair.getKey().toString()).equalTo(col(pair.getValue().toString())).and(n);
	        	n = "L."+ pair.getKey().toString() + " = " + "R."+ pair.getValue().toString() +" AND "+ n;
	        	}
	    }
		return n;
	}
	
	public ArrayList<String> DuplicateRemover()  {
		ArrayList<String> n = new ArrayList<String>();
		Iterator<?> it = this.ConditionOptr.entrySet().iterator();
	    while (it.hasNext()) {
	        @SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)it.next();
	         n.add( pair.getValue().toString() ); 
	    }
		return n;
	}
	
	public String  getSelectItem(String[] ActualCol)  {

		//ArrayList<String> result = new ArrayList<String>();
		String result = new String();
		//return this.SelectItem;
		//result.;
		for(String A : ActualCol){
			if(result.isEmpty()){
			result = result.concat("L."+ A +" AS "+ A + " ");
			}
			else {
				result = result.concat(", L."+ A +" AS "+ A + " ");
			}
		}
		Iterator<?> it = this.SelectItem.entrySet().iterator();
	    while (it.hasNext()) {
	        @SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + ":" + pair.getValue());
	        if(pair.getValue().toString().trim().length() == 0){
	        	if(result.isEmpty()){
	            result = result.concat(" " + pair.getKey().toString());
	        	 }
	        	else{
	        		result = result.concat(", " + pair.getKey().toString());
	        	}
	        }else{
	        	if(result.isEmpty()){
	        	result = result.concat(" R." +pair.getKey().toString() + " AS " + pair.getValue().toString());
	        	}
	        	else
	        	{
	        		result = result.concat(", R." +pair.getKey().toString() + " AS " + pair.getValue().toString());
	        	}
	        }
	    }
	    log.info("SELECT PATTERN : "+ result);
	    
	    
		return result;
	}
}