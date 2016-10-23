package com.penspark.steps.java_filters;

import org.apache.log4j.Logger;
import org.dom4j.Element;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class JavaFilterMeta  {
	static Logger log = Logger.getLogger(JavaFilterMeta.class);
	boolean option = true;
	String Filter_Condition = "";
	//String SqlOpr = "from ComOperation";
	public JavaFilterMeta(Element element) {
		 // allocate BaseStepMeta
		
		/*
		 * make this.SqlOpr to perfectly on the given Xml Operaton
		 */
		this.Filter_Condition = "";
	}


}