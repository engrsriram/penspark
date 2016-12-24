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

import com.esotericsoftware.minlog.Log;
import com.penspark.inits.Step.StepInType;
import com.penspark.steps.*;
import com.penspark.steps.add_const.AddConst;
import com.penspark.steps.filter_rows.FilterRows;
import com.penspark.steps.group_by.GroupBy;
import com.penspark.steps.select_values.SelectValues;
import com.penspark.steps.set_field_value.SetFieldValue;
import com.penspark.steps.set_field_value_const.SetFieldValueConst;
import com.penspark.steps.sort_rows.SortRows;
import com.penspark.steps.stream_lookup.StreamLookUp;
import com.penspark.steps.textfileinput.TextFileInput;
import com.penspark.steps.textfileoutput.TextFileOutput;
import com.penspark.steps.transdummy.transdummy;
import com.penspark.steps.stringoperations.StringOperations;
import com.penspark.steps.strings_cut.StringsCut;
import com.penspark.steps.switch_case.SwitchCase;

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
			case "SwitchCase":
				s.step = new SwitchCase(element);
				break;
			case "FilterRows":
				log.info("Filter Row found");
				s.step = new FilterRows(element);
				break;	
			case "SortRows":
				//log.info("Filter Row found");
				s.step = new SortRows(element);
				break;
			case "GroupBy":
				//log.info("Filter Row found");
				s.step = new GroupBy(element);
				break;
			case "SelectValues":
				//log.info("Filter Row found");
				s.step = new SelectValues(element);
				break;
			case "Constant":
				//log.info("Filter Row found");
				s.step = new AddConst(element);
				break;
			case "SetValueField":
				//log.info("Filter Row found");
				s.step = new SetFieldValue(element);
				break;
			case "SetValueConstant":
				//log.info("Filter Row found");
				s.step = new SetFieldValueConst(element);
				break;
			case "StringCut":
				//log.info("Filter Row found");
				s.step = new StringsCut(element);
				break;
			case "StreamLookup":				
				//log.info("LookUp Setup on the way");
				s.setStepInType(StepInType.Lookup);
				
				s.step = new StreamLookUp(element);
				log.info("Frop:Step LookUP"+s.step.getfrom());
				s.setFromStep(s.step.getfrom());
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
		log.info("Get Completed Result:"+name);
		Dataset<Row> Result = null;
    	for(Step s : bList)
    	{
    		ArrayList<String> l = s.getchildstep();
    		log.info(l.toString());
    		if(!l.isEmpty()){
    			if(l.contains(name)){
    			log.info("for Step:" +name+ "merging exising output with:"+ s.getName());
    			Result = DatasetOperations.combineset(Result , s.step.getOutput(name));
    			}
    		}
    	}
    	log.info("getting complete result");
    	return Result;
	}
	public Dataset<Row> GetLeftResult(String name) {
		/*
		 * This function will get output only from the "Non-fromStep" steps
		 * those are combined , then they are all to flow though the next
		*/ 
		
		
		log.info("Get LEFT Result:START:"+name);
		for(Step s : bList)
    	{
    		ArrayList<String> l = s.getchildstep();
    		if(!l.isEmpty()){
    			if(l.contains(name)  ){ // && (!s.is_fromstep_of(name))
    				
    					log.info(">'LeftResult'"+ s.getFromStep()+"//" + s.is_fromstep_of(name)+"<>" + name+"><"+l.toString());

    			}
    		}
    	}
		log.info("Get LEFT Result:END:"+name);
		Dataset<Row> Result = null;
    	for(Step s : bList)
    	{
    		ArrayList<String> l = s.getchildstep();
    		log.info(l.toString() + "|" + s.Name );
    		if(!l.isEmpty()){
    			if(l.contains(name)  ){ // && (!s.is_fromstep_of(name))
    				
    				if(!s.is_fromstep_of(name)){
    					log.info("Geting result Dataset of 'LeftResult'" + s.is_fromstep_of(name));
    					Result = DatasetOperations.combineset(Result , s.step.getOutput(name));
    					}
    			}
    		}
    	}
    	log.info("Showing Left Result-->");
    	Result.show();
    	return Result;
	}
	public Dataset<Row> GetRightResult(String name) {
		log.info("Get RIGHT Result:"+name);
		/// Or Otherwise Lookup Step
		// find the correct right from step Dataset and pass it as result.
		
		/*This function StreamLookUpMeta.fromStep1 name only should be picked up, 
		 * 
		 * 
		 * if fromStep1 matches with the parent step , 
		 * result of that step need to be used 
		 * 
		 * 
		 */
		
		
		for(Step s : bList)
    	{
    		ArrayList<String> l = s.getchildstep();
    		if(!l.isEmpty()){
    			if(l.contains(name)  ){ // && (!s.is_fromstep_of(name))
    				
    					log.info(">'RightResult'" + s.getFromStep()+"//" + s.is_fromstep_of(name)+"<>" + name+"><"+l.toString());

    			}
    		}
    	}
		
		Dataset<Row> Result = null;
    	for(Step s : bList)
    	{
    		ArrayList<String> l = s.getchildstep();
    		
    		
    		log.info(l.toString());
    		if(!l.isEmpty()){
    			if(l.contains(name)){
    				if(s.is_fromstep_of(name)){
    					log.info("Geting result Dataset of 'RightResult'");
    					Result = DatasetOperations.combineset(Result , s.step.getOutput(name));
    			}
    			}
    		}
    	}
    	log.info("getting Right Complete List result");
    	log.info("Showing Left Result-->");
    	Result.show();
    	return Result;
	}
	public Dataset<Row> GetCompletedResult(String name, String name2) {
		// TODO Auto-generated method stub
		return null;
	}
 
}