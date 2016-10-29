package com.penspark.steps.add_const;

import org.apache.log4j.Logger;
import org.dom4j.Element;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class AddConstMeta  {
	static Logger log = Logger.getLogger(AddConstMeta.class);
	public Element el;
	

	public AddConstMeta(Element element) {
		this.el = element;
	}
}