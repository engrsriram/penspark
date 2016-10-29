package com.penspark.steps.select_values;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.dom4j.Element;
import org.dom4j.Node;

import scala.Tuple3;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class SelectValuesMeta  {
	static Logger log = Logger.getLogger(SelectValuesMeta.class);
	
	ArrayList<Column> operAllcolmn =  new ArrayList<Column>();
	
	ArrayList<Column> operMetacolmn =  new ArrayList<Column>();
	
	ArrayList<String> operremove =  new ArrayList<String>();
	Map<String, String> optrMeta = new HashMap<String, String>();

	private Element etl;
	public SelectValuesMeta(Element el) {
		this.etl = el;
		
		@SuppressWarnings("unchecked")
		List<Node> nodes3 = el.selectNodes("fields/remove" );
		Iterator<Node> iter3=nodes3.iterator();
		while(iter3.hasNext()){
			Element element2=(Element)iter3.next();
			//this.Switchoptr.put(element2.elementText("target_step"), this.fieldfocus +" = '"+element2.elementText("value") +"'");
			//          <aggregate>count</aggregate>
	          //<subject>job_Title</subject>
	          //<type>COUNT_DISTINCT</type>
	          ///<valuefield/>
			//log.info(element2.asXML());
			
			log.info("Remove-name:" + element2.elementText("name"));
			this.operremove.add(element2.elementText("name"));
			
		}
		@SuppressWarnings("unchecked")
		List<Node> nodes4 = el.selectNodes("fields/meta" );
		Iterator<Node> iter4=nodes4.iterator();
		while(iter4.hasNext()){
			Element element2=(Element)iter4.next();
			//this.Switchoptr.put(element2.elementText("target_step"), this.fieldfocus +" = '"+element2.elementText("value") +"'");
			//          <aggregate>count</aggregate>
	          //<subject>job_Title</subject>
	          //<type>COUNT_DISTINCT</type>
	          ///<valuefield/>
			//log.info(element2.asXML());
			log.info("name:" + element2.elementText("name"));
			log.info("Remove-name:" + element2.elementText("rename"));
			log.info("type:" + element2.elementText("type"));
			log.info("length:" + element2.elementText("length"));
			log.info("precision:" + element2.elementText("precision"));
			this.optrMeta.put("type1", element2.elementText("type"));
			
		}
		
	}


	public ArrayList<Column> getalloper(String[] ActualCol)  {

		 log.info("ActualCol:"+Arrays.deepToString(ActualCol));
		// log.info("this.operend:"+Arrays.toString(this.operend.toArray()));
		for(String Coln : ActualCol){
			Column col1 = null ;
			 boolean Changed = false;
			 @SuppressWarnings("unchecked")
				List<Node> nodes2 = etl.selectNodes("fields/field" );
				Iterator<Node> iter2=nodes2.iterator();
				while(iter2.hasNext()){
					Element element2=(Element)iter2.next();
					log.info("name:" + element2.elementText("name"));
					log.info("rename:" +element2.elementText("rename"));
					//log.info("type:" + element2.elementText("type"));
					//log.info("valuefield:" +element2.elementText("valuefield"));
					log.info("length:" + element2.elementText("length"));
					log.info("precision:" + element2.elementText("precision"));			
				if(Coln.equals(element2.elementText("name")))	
					{
					if(element2.elementText("rename").isEmpty()){
						col1 = col(element2.elementText("name"));
						//log.info("Creating Name:" + element2.elementText("name"));
						Changed = true;					}
					else{
						//log.info("Creating Name & Re-name:" + element2.elementText("name") +":"+ element2.elementText("rename") );
						col1 = col(element2.elementText("name")).as(element2.elementText("rename"));
						Changed = true;
					}
					}
				}
			 
			if(!Changed) {
				col1 = col(Coln);
				
			}

			 this.operAllcolmn.add(col1);
			 
		 }
	
		return this.operAllcolmn;
	}
	
	public ArrayList<Column> getallMetaoper(String[] ActualCol)  {

		 log.info("ActualCol:"+Arrays.deepToString(ActualCol));
		// log.info("this.operend:"+Arrays.toString(this.operend.toArray()));
		for(String Coln : ActualCol){
			Column col1 = null ;
			 boolean Changed = false;
			 @SuppressWarnings("unchecked")
				List<Node> nodes2 = etl.selectNodes("fields/meta" );
				Iterator<Node> iter2=nodes2.iterator();
				log.info("ActualCol-name:" + Coln);
				
				while(iter2.hasNext()){
					Element element2=(Element)iter2.next();

					log.info("name:" + element2.elementText("name"));
					log.info("Remove-name:" + element2.elementText("rename"));
					log.info("type:" + element2.elementText("type"));
					log.info("length:" + element2.elementText("length"));
					log.info("precision:" + element2.elementText("precision"));
				if(Coln.equals(element2.elementText("name")))	
					{
					if(element2.elementText("rename").isEmpty()){
						col1 = col(element2.elementText("name")).cast(element2.elementText("type"));
						log.info("M.Creating Name:" + element2.elementText("name"));
						Changed = true;					}
					else{
						log.info("M.Creating Name & Re-name:" + element2.elementText("name") +":"+ element2.elementText("rename") );
						col1 = col(element2.elementText("name")).cast(element2.elementText("type")).as(element2.elementText("rename"));
						Changed = true;
					}
					}
				}
			 
			if(!Changed) {
				col1 = col(Coln);
				
			}
			this.operMetacolmn.add(col1);
			//if(!Changed) {
				 
			// }
			 
		 }
	
		return this.operMetacolmn;
	}
}