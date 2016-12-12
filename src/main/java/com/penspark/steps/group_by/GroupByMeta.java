package com.penspark.steps.group_by;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.max;
//import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.stddev_pop;


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
					//log.info("valuefield:" +element2.elementText("valuefield"));
					
					// if condition based on the type. and possible value field. 
				/*	if(element2.elementText("type").equals("COUNT_DISTINCT"))
					{
						this.operAgg.add(countDistinct(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					else if(element2.elementText("type").equals("COUNT_ALL"))
					{
						this.operAgg.add(count(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					*/

					//SUM --> Sum
					if(element2.elementText("type").equals("SUM"))
					{
						this.operAgg.add(sum(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//AVERAGE --> Average (Mean)
					else if(element2.elementText("type").equals("AVERAGE"))
					{
						this.operAgg.add(avg(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//MIN -->  Minimum
					else if(element2.elementText("type").equals("MIN"))
					{
						this.operAgg.add(min(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//MAX --> Maximum
					else if(element2.elementText("type").equals("MAX"))
					{
						this.operAgg.add(max(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//COUNT_ALL --> Number of Values (N)
					else if(element2.elementText("type").equals("COUNT_ALL"))
					{
						this.operAgg.add(count(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//CONCAT_COMMA --> Concatenate strings separated by ,
					else if(element2.elementText("type").equals("CONCAT_COMMA"))
					{
						this.operAgg.add(concat_ws(",",col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//FIRST -->  First non-null value
					else if(element2.elementText("type").equals("FIRST"))
					{
						this.operAgg.add(first(col(element2.elementText("subject")), true).as(element2.elementText("aggregate")));
					}
					//LAST -->  Last non-null value
					else if(element2.elementText("type").equals("LAST"))
					{
						this.operAgg.add(last(col(element2.elementText("subject")), true).as(element2.elementText("aggregate")));
					}
					//FIRST_INCL_NULL -->  First value
					else if(element2.elementText("type").equals("FIRST_INCL_NULL"))
					{
						this.operAgg.add(first(col(element2.elementText("subject")), false).as(element2.elementText("aggregate")));
					}
					//LAST_INCL_NULL -->  Last value
					else if(element2.elementText("type").equals("LAST_INCL_NULL"))
					{
						this.operAgg.add(last(col(element2.elementText("subject")), false).as(element2.elementText("aggregate")));
					}
					//CUM_SUM -->  Cumulative sum (all rows option only!) 
					else if(element2.elementText("type").equals("CUM_SUM"))
					{
						//this.operAgg.add(count(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//CUM_AVG -->  Cumulative average (all rows option only!)
					else if(element2.elementText("type").equals("CUM_AVG"))
					{
						//this.operAgg.add(count(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//STD_DEV -->  Standard deviation
					else if(element2.elementText("type").equals("STD_DEV"))
					{
						this.operAgg.add(stddev_pop(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//CONCAT_STRING -->  Concatenate strings separated by // <valuefield>er</valuefield>
					else if(element2.elementText("type").equals("CONCAT_STRING"))
					{
						this.operAgg.add(concat_ws(element2.elementText("valuefield"), col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
					}
					//COUNT_DISTINCT -->  Number of Distinct Values (N)
					else if(element2.elementText("type").equals("COUNT_DISTINCT"))
					{
						this.operAgg.add(countDistinct(col(element2.elementText("subject"))).as(element2.elementText("aggregate")));
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