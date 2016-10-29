package com.penspark.steps.switch_case;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class SwitchCaseMeta  {
	static Logger log = Logger.getLogger(SwitchCaseMeta.class);
	ArrayList<String> Destinations = new ArrayList<String>();
	ArrayList<String> Operatators = new ArrayList<String>();
	String fieldfocus = "";
	String default_step = "";
	Map<String, String> Switchoptr = new HashMap<String, String>();
	public SwitchCaseMeta(Element el)
	{
		log.info("Field to operate:"+ el.elementText("fieldname"));
		this.fieldfocus = el.elementText("fieldname");
		log.info("Defaulting step:" + el.elementText("default_target_step"));
		this.default_step = el.elementText("default_target_step");
		@SuppressWarnings("unchecked")
		List<Node> nodes1 = el.selectNodes("cases/case" );
		Iterator<Node> iter1=nodes1.iterator();
		while(iter1.hasNext()){
			Element element2=(Element)iter1.next();
			this.Switchoptr.put(element2.elementText("target_step"), this.fieldfocus +" = '"+element2.elementText("value") +"'");
			this.Destinations.add(element2.elementText("target_step"));
			if(el.elementText("use_contains").equals("Y")){
				this.Operatators.add(this.fieldfocus +" LIKE '%"+element2.elementText("value") +"%'");}
			else {
				this.Operatators.add(this.fieldfocus +" = '"+element2.elementText("value") +"'");}
			}
		}
	}


