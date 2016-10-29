package com.penspark.steps.group_by;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.dom4j.Element;
import org.dom4j.Node;

/**
 * This class takes care of the meta data for the StringOperations step.
 * 
 */
class GroupByMeta  {
	static Logger log = Logger.getLogger(GroupByMeta.class);
	
	ArrayList<Column> operGroupBy =  new ArrayList<Column>();
	ArrayList<Column> operAgg =  new ArrayList<Column>();

	public GroupByMeta(Element el) {
				@SuppressWarnings("unchecked")
				List<Node> nodes1 = el.selectNodes("group/field" );
				Iterator<Node> iter1=nodes1.iterator();
				while(iter1.hasNext()){
					Element element2=(Element)iter1.next();
					//log.info("Groupby:"+element2.elementText("name"));
					this.operGroupBy.add(col(element2.elementText("name")));
				}
				@SuppressWarnings("unchecked")
				List<Node> nodes2 = el.selectNodes("fields/field" );
				Iterator<Node> iter2=nodes2.iterator();
				while(iter2.hasNext()){
					Element element2=(Element)iter2.next();
					log.info("aggregate:" + element2.elementText("aggregate"));
					log.info("subject:" +element2.elementText("subject"));
					log.info("type:" + element2.elementText("type"));
					log.info("valuefield:" +element2.elementText("valuefield"));
					
					// if condition based on the type. and possible value field. 
					if(element2.elementText("type").equals("COUNT_DISTINCT"))
					{
						this.operAgg.add(count(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					
				}
	}


	public ArrayList<Column> getGroupby()  {

		return this.operGroupBy;
	}
	public ArrayList<Column> getAgg()  {

		return this.operAgg;
	}
}