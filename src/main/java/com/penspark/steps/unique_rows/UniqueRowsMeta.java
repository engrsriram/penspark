package com.penspark.steps.unique_rows;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class UniqueRowsMeta  {
	static Logger log = Logger.getLogger(UniqueRowsMeta.class);
	
	ArrayList<String> operMaster =  new ArrayList<String>();

	public UniqueRowsMeta(Element element) {
		
		@SuppressWarnings("unchecked")
		List<Node> nodes2 = element.selectNodes("fields/field");
        Iterator<Node> iter2=nodes2.iterator();
        	while(iter2.hasNext()){
        		Element element2=(Element)iter2.next();
        		//System.out.println("String operation:"+element2.asXML());
        		this.operMaster.add(element2.elementText("name"));
        	}
	}

	public ArrayList<String> getUniqOpr() {
		return this.operMaster;
	}
	
}