package com.penspark.steps.closure_generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class ClosureGeneratorMeta  {
	static Logger log = Logger.getLogger(ClosureGeneratorMeta.class);
	
	ArrayList<String> operMaster =  new ArrayList<String>();
	ArrayList<String> operend =  new ArrayList<String>();

	public ClosureGeneratorMeta(Element element) {
		
		@SuppressWarnings("unchecked")
		List<Node> nodes3 = element.selectNodes("type");
		for(Node W: nodes3)
		{
			//.selectNodes("fields/field")
			log.info("Verify Type" + W.getStringValue());			
		}
		
		@SuppressWarnings("unchecked")
		List<Node> nodes2 = element.selectNodes("fields/field");
        Iterator<Node> iter2=nodes2.iterator();
        	while(iter2.hasNext()){
        		Element element2=(Element)iter2.next();
        		System.out.println("String operation:"+element2.asXML());
        		
        		String result = (element2.elementText("in_stream_name") != null)?element2.elementText("in_stream_name") : "";
        		String n = result.toString();
    			//System.out.println("field:"+element2.elementText("name"));
        		//System.out.println("field:"+element2.elementText("type"));
        		//System.out.println("field:"+element2.elementText("decimal"));
        		log.info("Result:" + result + ", n:" + n);
        		log.info("trim_type" + element2.elementText("trim_type"));
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
 				log.info("lower_upper" + element2.elementText("lower_upper"));
				if(!element2.elementText("lower_upper").isEmpty()){
 					switch (element2.elementText("lower_upper")) {
					case "lower":
						result = "LOWER ( " + result + " )";
						break;
					case "upper":
						result = "UPPER ( " + result + " )";
						break;
					default:
						break;
					}					
				}
				log.info("padding_type" + element2.elementText("padding_type") + "<>" +"pad_char" + element2.elementText("pad_char")+ "<>" +"field/pad_len" + element2.elementText("field/pad_len"));
				if(!(element2.elementText("padding_type").isEmpty() ) && (element2.elementText("pad_char").isEmpty()) && (element2.elementText("pad_len").isEmpty()) ){
					
				}
				log.info("init_cap" + element2.elementText("init_cap"));
				if(element2.elementText("init_cap").equals("yes")){
					result = "concat(ucase(substr( " + result + " , 1,1)), substr(" + result + ", 2))"; 
				}
				log.info("mask_xml" + element2.elementText("mask_xml"));
				if(element2.elementText("mask_xml") != null){
					
				}
				log.info("digits" + element2.elementText("digits"));
				if(!element2.elementText("digits").isEmpty()){
					
				}
				log.info("remove_special_characters" + element2.elementText("remove_special_characters"));
				if(!element2.elementText("remove_special_characters").isEmpty()){
					
				}
				log.info("out_stream_name" + element2.elementText("out_stream_name"));
				if(element2.elementText("out_stream_name").isEmpty()){
        			result  = result + " as " + n.toString();
				} else {
					result  = result + " as " + element2.elementText("out_stream_name");
        		}
				
				log.info("Generated Result:" + result);
        		this.operend.add(result);
        	}
		
	}


	public ArrayList<String> getalloperator(String[] ActualCol)  {

		 log.info("ActualCol:"+Arrays.deepToString(ActualCol));
		 log.info("this.operend:"+Arrays.toString(this.operend.toArray()));
		for(String Coln : ActualCol){
			 boolean Changed = false;
			for(String op : this.operend)
			 {
				 
				 
				 // find last word in this.operend 
				 // check whether it matches with ActualCol. 
				 // if it matches then add.operMaster with this.operend
				 //else copy ActualCol to operMaster

				 String op1 = op.substring(op.lastIndexOf(" ")+1);
				 if(Coln.equals(op1)){
					 this.operMaster.add(op);
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