package com.penspark.steps.strings_cut;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

import com.penspark.inits.xml.Utils;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class StringsCutMeta  {
	static Logger log = Logger.getLogger(StringsCutMeta.class);
	
	ArrayList<String> operMaster =  new ArrayList<String>();
	ArrayList<String> operend =  new ArrayList<String>();
	Element el;
	public StringsCutMeta(Element element) {
		
		this.el = element;
		
	}


	public ArrayList<String> getalloperator(String[] ActualCol)  {

		 log.info("ActualCol:"+Arrays.deepToString(ActualCol));
		// log.info("this.operend:"+Arrays.toString(this.operend.toArray()));
		for(String Coln : ActualCol){
			 boolean Changed = false;
			 @SuppressWarnings("unchecked")
				List<Node> nodes2 = this.el.selectNodes("fields/field");
		        Iterator<Node> iter2=nodes2.iterator();
		        	while(iter2.hasNext()){
		        		Element element2=(Element)iter2.next();
		        		//System.out.println("String operation:"+element2.asXML());
		        		if(element2.elementText("in_stream_name").equals(Coln)){
	        				int start = Utils.getItsInt(element2.elementText("out_stream_name"));
	        				int to = Utils.getItsInt(element2.elementText("out_stream_name"));
	        				int le = (start < to )? (to - start) : 0;
	        				String result = element2.elementText("in_stream_name");
	        				if(start != 0 && le != 0){
	        					result = "substr("+ result + ", "+ start +", "+ le+")";
	        				}
	        				else if(start != 0){
	        					result = "substr("+ result + ", "+ start +")";
	        				}
		        			if(!(element2.elementText("out_stream_name").isEmpty())){
		        			// has output field options
		        				this.operMaster.add(result + " AS " + element2.elementText("out_stream_name"));

		        			} 
		        			else
		        			{
		        				this.operMaster.add(result);
		        			}
		        			Changed = true;
		        		}
		        		
		        	}
			if(!Changed) {
				 this.operMaster.add(Coln);
			 }
			 
		 }
	
		return this.operMaster;
	}
	
}