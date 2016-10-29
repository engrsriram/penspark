package com.penspark.steps.set_field_value_const;

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
class SetFieldValueConstMeta  {
	static Logger log = Logger.getLogger(SetFieldValueConstMeta.class);
	
	Element el = null;
	public SetFieldValueConstMeta(Element element) {
	this.el = element;
	}
}