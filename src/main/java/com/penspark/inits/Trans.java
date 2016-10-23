package com.penspark.inits;

import org.apache.log4j.Logger;
//import org.jaxen.*;
import org.dom4j.*;


import com.penspark.players.Player;
public class Trans{
	static Logger log = Logger.getLogger(Trans.class);
	
	ListofHopes Hopes;
	//ListofHopes Hopes = new ListofHopes();

	ListStep Steps;
  
	public Player player;
	
	private boolean valid;
	public Trans(Element classElement) {
		// TODO Auto-generated constructor stub
		log.info("in Trans Constructor . getting Details. ");
		Hopes = new ListofHopes(classElement);
		//ListofHopes Hopes = new ListofHopes();
		valid = false;
		Steps = new ListStep(classElement);
	
		player = new Player(Hopes ,Steps );	
            // System.out.println(element.asXML());
        }
	


	public boolean valid() {
		// TODO Auto-generated method stub
		return this.valid;
	}
            
		// do something
}
	
