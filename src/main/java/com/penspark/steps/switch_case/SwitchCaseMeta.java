package com.penspark.steps.switch_case;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.dom4j.Element;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class SwitchCaseMeta  {
	static Logger log = Logger.getLogger(SwitchCaseMeta.class);
	ArrayList<String> Destinations = new ArrayList<String>();
	ArrayList<String> Operatators = new ArrayList<String>();
	public SwitchCaseMeta(Element element) {
		 // allocate BaseStepMeta
		
		/*
		 * make this.SqlOpr to perfectly on the given Xml Operaton
		 */
	}


}