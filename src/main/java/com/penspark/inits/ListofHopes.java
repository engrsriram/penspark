package com.penspark.inits;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

public class ListofHopes implements Iterable<TransHopes> {
	static Logger log = Logger.getLogger(ListofHopes.class);
    private final List<TransHopes> bList = new ArrayList<TransHopes>();

    @Override
    public Iterator<TransHopes> iterator() {
        return bList.iterator();
    }

    ListofHopes(Element classElement)
    {
    	@SuppressWarnings("unchecked")
		List<Node> nodes = classElement.selectNodes("//transformation/order/hop" );
		Iterator<Node> iter=nodes.iterator();
		while(iter.hasNext()){
            Element element=(Element)iter.next();
            TransHopes t = new TransHopes(element);
        	bList.add(t);

        }
    }
 
}