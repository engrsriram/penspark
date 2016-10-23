package com.penspark.steps.commonoperations;

import org.apache.log4j.Logger;
import org.dom4j.Element;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class CommonOperationsMeta  {
	static Logger log = Logger.getLogger(CommonOperationsMeta.class);

	String SqlOpr = "from ComOperation";
	public CommonOperationsMeta(Element element) {
		 // allocate BaseStepMeta
		
		/*
		 * make this.SqlOpr to perfectly on the given Xml Operaton
		 */
	}


}