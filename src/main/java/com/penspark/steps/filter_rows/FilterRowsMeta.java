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
	
	String result = "";
	log.info(element.elementText("send_true_to"));
	log.info(element.elementText("send_false_to"));
	//log.info(element1.asXML());
	@SuppressWarnings("unchecked")
	List<Node> nodes1 = element.selectNodes("compare/condition" );
	log.info(nodes1.size());
	Element onlycon =(Element)nodes1.get(0);
	log.info(onlycon.elementText("negated"));
	Iterator<Node> iter1=nodes1.iterator();
	while(iter1.hasNext()){
		Element element2=(Element)iter1.next();
		// /conditions/condition 
		// if the iter1 contains conditions then pass to conditions(element)
		// else pass same to condition(element)
		log.info(element2.elementText("negated"));
		if(element2.elements("conditions").size()> 0){
			log.info("<<>>");
			List<Node> nodesqw = element2.selectNodes("conditions" );
			log.info(nodesqw.size());
	        Iterator<Node> iter2=nodes1.iterator();
			while(iter2.hasNext()){
				Element element3=(Element)iter2.next();
				result += reCondition(element3);
			}
		}
		else {
			log.info("condition");
		}
	}
	this.Filter_Condition = result;
	log.info(this.Filter_Condition);
	//return result;
}
private String reCondition(Element element2) {
	String result = "";

	log.info("condition:"+ element2.getName());
	if(element2.getName().equals("condition")){
		@SuppressWarnings("unchecked")
		List<Node> nodes1 = element2.selectNodes("conditions" );
		log.info(":>:"+nodes1.size());
        Iterator<Node> iter1=nodes1.iterator();
		while(iter1.hasNext()){
			
			log.info("IM HR");
			

			log.info("conditions+"+ element2.asXML());
            Element element4=(Element)iter1.next();
            log.info("C"+element4.elements("conditions").size());
            if(element4.elements("conditions").size()> 0){
            	result += reCondition(element4);
            	log.info(result);           		
            }
            else {
            	result += NodeCondition(element4);
            	
            	
         
            	
            	log.info(result); 
            }
        }	
		//result += reCondition(element2);
	}
	else{
		log.info("Its Condition");
		result += reCondition(element2);
	}
//	if(element2.elementText("negated").equals("N"))
//	{
//		log.info(element2.getPath());
//		result = "(" + result + ")";	
//	}else{
//		result = "NOT (" + result + ")";	
//	}
	if(element2.elementText("negated").equals("Y"))
	{
		result = "NOT (" + result + ")";	
	}
	log.info("RESULT" + result);
	return result;
}
private String NodeCondition(Element element1) {
	String result = "";
	@SuppressWarnings("unchecked")
	//List<Node> nodes1 = element1.selectNodes("compare/condition/conditions/condition" );
	List<Node> nodes1 = element1.selectNodes("condition" );
	log.info(nodes1.size());
    Iterator<Node> iter1=nodes1.iterator();
	while(iter1.hasNext()){
        Element element2=(Element)iter1.next();
        if(element2.elements("conditions").size()> 0){
        	//result += reCondition(element2);
        	
        	log.info(element2.elements("operator").size());
        	log.info(element2.elementText("operator"));
        	if(element2.elements("operator").size()> 0){
        		String pre = " " + element2.elementText("operator") + " ";
        		result += pre +"("+ reCondition(element2) + ")";
        	}
        	log.info(result);           		
        } 
        else
        	{
        log.info("Final:" + element2.asXML());
        log.info("-->:" +element2.elements("rightvalue").size());
    	if(element2.elements("rightvalue").size()> 0){
    		log.info(element2.elementText("rightvalue"));
    		if(!element2.elementText("rightvalue").isEmpty()){
        	result += rightCol(element2);
        	
        	log.info(result);  
    	 }
        else {
        	result += rightColvalue(element2);
        	log.info(result); 
        }
        }
        else {
        	result += rightColvalue(element2);
        	log.info(result); 
        }
    	}
    }
	//return result;
	return result;
}
private String rightCol(Element element2) {
	String result = "";
	String pre = "";
	
	switch (element2.elementText("function")) {
	case "IS NOT NULL":
	case "IS NULL":
		result +=  element2.elementText("leftvalue") + " " +element2.elementText("function") + " ";
		break;
	case "CONTAINS":
		result += element2.elementText("leftvalue") + " like CONCAT('%'"+ element2.elementText("rightvalue") + "'%')";
		break;
	case "STARTS WITH":
		result += element2.elementText("leftvalue") + " like CONCAT("+ element2.elementText("rightvalue") + "'%')";
		break;
	case "ENDS WITH":
		result += element2.elementText("leftvalue") + " like CONCAT('%'"+ element2.elementText("rightvalue") + ")";
		break;
	default:
		result += element2.elementText("leftvalue") + " " +element2.elementText("function") + " "+ element2.elementText("rightvalue"); 
	}
	
	if(element2.elements("operator").size()> 0){
		pre = " " + element2.elementText("operator") + " ";
		result  = pre +"("+ result + ")";
	} else{
		result = " (" + result + ")";		
	}
	if(element2.elementText("negated").equals("Y"))
	{
		result = "NOT (" + result + ")";	
	}
	return result;

}
private String rightColvalue(Element element2) {
	String result="";
	String pre = "";
	String RightValue = getRightValue(element2);
	log.info("Value:" + element2.getPath());
	log.info(element2.asXML());
	switch (element2.elementText("function")) {
	case "IS NOT NULL":
	case "IS NULL":
		result +=  element2.elementText("leftvalue") + " " +element2.elementText("function") + " ";
		break;
	case "CONTAINS":
		result += element2.elementText("leftvalue") + " like '%"+ RightValue + "%'";
		break;
	case "STARTS WITH":
		result += element2.elementText("leftvalue") + " like '" + RightValue + "%'";
		break;
	case "ENDS WITH":
		result += element2.elementText("leftvalue") + " like '%'"+ RightValue + "'" ;
		break;
	default:
		result += element2.elementText("leftvalue") + " " +element2.elementText("function") + " '" + RightValue + "'"; 
	}
	if(element2.elements("operator").size()> 0){
		pre = " " + element2.elementText("operator") + " ";
		result  = pre +"("+ result + ")";
	} else{
		result = " (" + result + ")";		
	}
	if(element2.elementText("negated").equals("Y"))
	{
		result = "NOT (" + result + ")";	
	}
	return result;

}

private String getRightValue(Element element2) {
	String result = "";
	@SuppressWarnings("unchecked")
	List<Node> nodes1 = element2.selectNodes("value" );
	log.info(nodes1.size());
    Iterator<Node> iter1=nodes1.iterator();
	while(iter1.hasNext()){
        Element element22=(Element)iter1.next();
        result = element22.elementText("text");
	}
	return result;
}

	public String getFltrCondition() {
		return this.Filter_Condition;
	}


}