package com.penspark.inits;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.dom4j.Element;
import org.dom4j.Node;

import com.penspark.steps.StepInterface;

/*
 * Todo:
 * Read all the details in given trans file and make it as a meta data object for further processing
 */
public class Step {
	 static Logger log = Logger.getLogger(Step.class);



	int No_inHops = 0;
	int No_OutHops = 0;
	String Name;
	String Type;

	public StepInterface step;
	
	//Set<String> parentstep = new HashSet<String>();
	ArrayList<String> parentstep = new ArrayList<String>();
	//Set<String> childstep = new HashSet<String>();
	ArrayList<String> childstep = new ArrayList<String>();

	Boolean Distribute = false;
	//Dataset<Row> Input;
	//Dataset<Row> Output;
	
	public enum Status {Waiting, Running, Completed , Failed};
	public enum StepType {Zero_to_Zero , Zero_to_one , one_to_Zero , One_to_One, One_to_Many, Many_to_One , Many_to_Many};
	public enum StepInType {Normal , Lookup };
	StepInType  CurrentInputType = StepInType.Normal;
	Status CurrentStatus = Status.Waiting;
	StepType CurrentStepType = StepType.Zero_to_Zero;
	Map<String, String> Column_List ;
	
	protected Step(Element classElement)
	{
        @SuppressWarnings("unchecked")
			List<Node> nodes2 = classElement.selectNodes("fields/field");
           Iterator<Node> iter2=nodes2.iterator();
           	while(iter2.hasNext()){
           		Element element2=(Element)iter2.next();
           		//System.out.println("field:"+element2.asXML());
           		
           		
           	}
           	
		
	}
	
	
	public String getName() {
		return Name;
	}


	public void setName(String name) {
		Name = name;
	}
	
	public StepInType getstepInType()
	{
		return this.CurrentInputType;
	}
	public void setStepInType(StepInType r)
	{
		this.CurrentInputType = r;
	}
	public String getType() {
		return Type;
	}


	public void setType(String type) {
		this.Type = type;
	}
	public int getNo_inHops() {
		return No_inHops;
	}


	public void setNo_inHops(int no_inHops) {
		this.No_inHops = no_inHops;
	}
	public void incNo_inHops() {
		this.No_inHops++;
	}

	public int getNo_OutHops() {
		return No_OutHops;
	}


	public void setNo_OutHops(int no_OutHops) {
		this.No_OutHops = no_OutHops;
	}
	public void incNo_OutHops() {
		this.No_OutHops++;
	}
	public void show(){
		log.info("Get .ktr file and variables. " + Status.Waiting);
	}


	
	// parent child  step geting though.
	//@SuppressWarnings("unchecked")
	public ArrayList<String> getparentstep(){
		return this.parentstep;
		
	}
	public ArrayList<String> getchildstep(){
		//return (ArrayList<String>) this.childstep;
		return this.childstep;
	}
	
	// adding values
	public void addparentstep(String pStep)
	{
		this.parentstep.add(pStep);
	}
	
	public void addchildstep(String cStep)
	{
		this.childstep.add(cStep);
	}
	
	public Status getStatus() {
		return this.CurrentStatus;
	}
	public boolean is_completed() {
		log.info("Checking is Completed :" + this.Name + ":" + this.CurrentStatus);
		if(this.CurrentStatus == Status.Completed) 
			return true;
		else
			return false;
	}
	public void SetStatus(Status s) {
		this.CurrentStatus = s;
	}
}
