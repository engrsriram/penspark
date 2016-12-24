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
	String fromStep1 = null;
	public StreamLookUpMeta(Element element) {
		this.fromStep1 = element.elementText("from");
		@SuppressWarnings("unchecked")
		List<Node> nodes1 = element.selectNodes("lookup/key" );
		Iterator<Node> iter1=nodes1.iterator();
		while(iter1.hasNext()){
			Element element2=(Element)iter1.next();
			this.ConditionOptr.put(element2.elementText("name"), element2.elementText("field") +"'");
		}
		
		@SuppressWarnings("unchecked")
		List<Node> nodes11 = element.selectNodes("lookup/value" );
		Iterator<Node> iter11=nodes11.iterator();
		while(iter11.hasNext()){
			Element element2=(Element)iter11.next();
			this.SelectItem.put(element2.elementText("name"), element2.elementText("rename") +"'");
		}
		
		
		
	}
	

	public Column getConditionOptr()  {
		//String[] ActualCol
		Column n = null ;
		//return this.ConditionOptr;
		Iterator<?> it = this.ConditionOptr.entrySet().iterator();
	    while (it.hasNext()) {
	        @SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)it.next();
	       // System.out.println(pair.getKey() + " : " + pair.getValue());
	        if(n == null)
	        	{
	        	n = col(pair.getKey().toString()).equalTo(col(pair.getValue().toString()));
	        	}
	        else
	        	{
	        	n = col(pair.getKey().toString()).equalTo(col(pair.getValue().toString())).and(n);
	        	}
	    }
		return n;
	}
	public ArrayList<String>  getSelectItem(String[] ActualCol)  {

		ArrayList<String> result = new ArrayList<String>();
		//return this.SelectItem;
		//result.;
		for(String A : ActualCol){
			result.add(A);
		}
		Iterator<?> it = this.SelectItem.entrySet().iterator();
	    while (it.hasNext()) {
	        @SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " : " + pair.getValue());
	        if(pair.getValue().toString().trim().length() == 0){
	            result.add(pair.getKey().toString());
	        }else{
	        	result.add(pair.getKey().toString() + " AS " + pair.getValue().toString());
	        }
	    }
		return result;
	}
}