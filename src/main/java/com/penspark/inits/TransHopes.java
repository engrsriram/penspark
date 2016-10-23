package com.penspark.inits;

import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;


public class TransHopes {
	static Logger log = Logger.getLogger(TransHopes.class);
	private static Class<?> PKG = TransHopes.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	public static final String XML_TAG = "hop";

	private boolean enabled;

	public boolean split = false;

	private boolean changed;

	private String from_step;
	private String to_step;
	
	protected TransHopes(Element classElement)
	{/*
		log.info("FROM:"+classElement.selectSingleNode("from").getText());
        log.info("To:"+classElement.selectSingleNode("to").getText());
        log.info("enabled:"+classElement.selectSingleNode("enabled").getText());
        log.info(classElement.asXML());
		*/
		this.from_step = classElement.selectSingleNode("from").getText();
		this.to_step = classElement.selectSingleNode("to").getText();
		this.enabled = classElement.selectSingleNode("enabled").getText().equals("Y");
	}


	public void setFromStep(String from)
	{
		 from_step = from;
	}

	public void setToStep(String to)
	{
		 to_step = to;
	}

	public String getFromStep()
	{
		return from_step;
	}

	public String getToStep()
	{
		return to_step;
	}

	private Element searchStep(List<Element> steps, String name)
	{
		for (Element stepMeta : steps)
			if (stepMeta.getName().equalsIgnoreCase(name))
				return stepMeta;

		return null;
	}

	public Object clone()
	{
		try
		{
			Object retval = super.clone();
			return retval;
		} catch (CloneNotSupportedException e)
		{
			return null;
		}
	}


	/**
	 * Compare 2 hops.
	 */
	public int compareTo(Element obj)
	{
		return toString().compareTo(obj.toString());
	}

	public void setChanged()
	{
		setChanged(true);
	}

	public void setChanged(boolean ch)
	{
		changed = ch;
	}

	public boolean hasChanged()
	{
		return changed;
	}

	public void setEnabled()
	{
		setEnabled(true);
	}

	public void setEnabled(boolean en)
	{
		enabled = en;
		setChanged();
	}

	public boolean isEnabled()
	{
		return enabled;
	}


}
