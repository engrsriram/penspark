package com.penspark.inits;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.dom4j.Element;
import org.dom4j.Node;

import com.penspark.steps.*;
import com.penspark.steps.filter_rows.FilterRows;
import com.penspark.steps.textfileinput.TextFileInput;
import com.penspark.steps.textfileoutput.TextFileOutput;
import com.penspark.steps.transdummy.transdummy;
import com.penspark.steps.stringoperations.StringOperations;

/*
 * Todo:
 * Read all the details in given trans file and make it as a meta data object for further processing
 */
public class ListStep implements Iterable<Step> {
	static Logger log = Logger.getLogger(ListStep.class);
	private final List<Step> bList = new ArrayList<Step>();

    @Override
    public Iterator<Step> iterator() {
        return bList.iterator();
    }
    ListStep(Element classElement)
    {
    	@SuppressWarnings("unchecked")
		List<Node> nodes1 = classElement.selectNodes("//transformation/step" );
		
		Iterator<Node> iter1=nodes1.iterator();
		while(iter1.hasNext()){
            Element element=(Element)iter1.next();
            
            //Node file = element.selectSingleNode("fields/field");
            Step s = new Step(element);
            s.setName(element.selectSingleNode("name").getText());
            s.setType(element.selectSingleNode("type").getText());
             //Stepping st = new Stepping();
            switch (s.getType()) {
			case "TextFileInput":
				s.step = new TextFileInput(element);
				break;
			case "StringOperations":
				s.step = new StringOperations(element);
				break;
			case "TextFileOutput":
				s.step = new TextFileOutput(element);
				break;				
			case "FilterRows":
				log.info("Filter Row found");
				s.step = new FilterRows(element);
				break;	
			default:
				s.step = new transdummy(element);
				break;
			}
            bList.add(s);
            }    
    }
    public boolean CheckparrentCompleted(String StepName){
    	boolean desided = true;
    	for(Step s : bList)
    	{
    		// iterate though all the object in List and find which is  parameter. and check whether the step is completed or not. 
    		if(s.getchildstep().contains(StepName)){
    			desided = desided & s.is_completed();
    			
    		}
    	}
    	return desided;
    }
	public boolean checkworking() {
		 for (Step p : bList) {
		        if (!p.is_completed()) {
		            return false;
		        }
		    }
		return true;
	}
	public Dataset<Row> GetCompletedResult(String name) {
		Dataset<Row> Result = null;
    	for(Step s : bList)
    	{
    		ArrayList<String> l = s.getchildstep();
    		log.info(l.toString());
    		if(!l.isEmpty()){
    			if(l.contains(name)){
    			Result = DatasetOperations.combineset(Result , s.step.getOutput(name));
    			}
    		}
    	}
    	log.info("getting complete result");
    	return Result;
	}
	public Dataset<Row> GetCompletedResult(String name, String name2) {
		// TODO Auto-generated method stub
		return null;
	}
 
}