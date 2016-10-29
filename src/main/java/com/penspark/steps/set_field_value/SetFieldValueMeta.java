package com.penspark.steps.set_field_value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

class SetFieldValueMeta  {
	static Logger log = Logger.getLogger(SetFieldValueMeta.class);
	
	ArrayList<String> operMaster =  new ArrayList<String>();
	ArrayList<String> operend =  new ArrayList<String>();

	public Element el;

	public SetFieldValueMeta(Element element) {
		
	this.el = element;
	}
}