package com.penspark.steps.filter_rows;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class FilterRowsMeta  {
	static Logger log = Logger.getLogger(FilterRowsMeta.class);
	String option = "";
	String Filter_Condition = "";
	//String SqlOpr = "from ComOperation";
	public FilterRowsMeta(Element element) {
		 // allocate BaseStepMeta
		
		/*
		 * get True and false options. 
		 * 
		 * 
		 */
		log.info(element.asXML());
		log.info(element.elementText("send_true_to"));
		log.info(element.elementText("send_false_to"));
		this.option = element.elementText("send_false_to");
		
		@SuppressWarnings("unchecked")
		List<Node> compare = element.selectNodes("compare"); //$NON-NLS-1$
    	@SuppressWarnings("unchecked")
    	List<Node> condnode =  element.selectNodes("condition"); //$NON-NLS-1$
    	for(Node W: compare)
		{
			//.selectNodes("fields/field")
			log.info("Verify Type" + W.getStringValue());			
		}
    	
    	/*
		// The new situation...
		if (condnode)
		{
			condition = new Condition(condnode);
		}
		else // Old style condition: Line1 OR Line2 OR Line3: @deprecated!
		{
			condition = new Condition();
			
			int nrkeys   = XMLHandler.countNodes(compare, "key"); //$NON-NLS-1$
			if (nrkeys==1)
			{
				Node knode = XMLHandler.getSubNodeByNr(compare, "key", 0); //$NON-NLS-1$
				
				String key         = XMLHandler.getTagValue(knode, "name"); //$NON-NLS-1$
				String value       = XMLHandler.getTagValue(knode, "value"); //$NON-NLS-1$
				String field       = XMLHandler.getTagValue(knode, "field"); //$NON-NLS-1$
				String comparator  = XMLHandler.getTagValue(knode, "condition"); //$NON-NLS-1$

				condition.setOperator( Condition.OPERATOR_NONE );
				condition.setLeftValuename(key);
				condition.setFunction( Condition.getFunction(comparator) );
				condition.setRightValuename(field);
				condition.setRightExact( new ValueMetaAndData("value", value ) ); //$NON-NLS-1$
			}
			else
			{
				for (int i=0;i<nrkeys;i++)
				{
					Node knode = XMLHandler.getSubNodeByNr(compare, "key", i); //$NON-NLS-1$
					
					String key         = XMLHandler.getTagValue(knode, "name"); //$NON-NLS-1$
					String value       = XMLHandler.getTagValue(knode, "value"); //$NON-NLS-1$
					String field       = XMLHandler.getTagValue(knode, "field"); //$NON-NLS-1$
					String comparator  = XMLHandler.getTagValue(knode, "condition"); //$NON-NLS-1$
					
					Condition subc = new Condition();
					if (i>0) subc.setOperator( Condition.OPERATOR_OR   );
					else     subc.setOperator( Condition.OPERATOR_NONE );
					subc.setLeftValuename(key);
					subc.setFunction( Condition.getFunction(comparator) );
					subc.setRightValuename(field);
					subc.setRightExact( new ValueMetaAndData("value", value ) ); //$NON-NLS-1$
					
					condition.addCondition(subc);
				}
			}
			
			*/
		this.Filter_Condition = "";
		log.info(this.Filter_Condition);
	}
	public String getFltrCondition() {
		return this.Filter_Condition;
	}


}