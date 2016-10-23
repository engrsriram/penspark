package com.penspark.players;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import com.penspark.inits.ListStep;
import com.penspark.inits.ListofHopes;
import com.penspark.inits.Step;
import com.penspark.inits.Step.Status;
import com.penspark.inits.TransHopes;

public class Player extends PlayerMeta {
	static Logger log = Logger.getLogger(Player.class);
	ListofHopes hopes;
	ListStep steps;
	
	ListStep Arrangedsteps;
	//ArrayList<StepInterface> LSteps;

	public Player(ListofHopes hopes, ListStep steps) {
		// TODO Auto-generated constructor stub
		this.hopes = hopes;
		this.steps = steps;
		// Asign no of in_hopes and out_hopes for each step. 
		for(Step s : this.steps)
		{
			// get Step name and loop along with the all the Hopes(form) and deside no of out_hopes are there. 
			String Hint = s.getName();
			for(TransHopes h : this.hopes)
			{
				if(h.getFromStep().equals(Hint) && h.isEnabled())
				{
					s.incNo_OutHops();
					
					s.addchildstep(h.getToStep());
				}
				if(h.getToStep().equals(Hint) && h.isEnabled())
				{
					s.incNo_inHops();
					s.addparentstep(h.getFromStep());
				}
				
			}
			// Find its next step and append result set in 
		}
		
		
		
	}
	
	public void managehopes()
	{
		// List all the Hopes as per the order and the copy
		
		// TEST: validate atleast one Step with ZERO in_hopes , and atleast one step which ZERO out_hopes  in given list.
		
		log.info("all parrent /child step:");
		for (Step h : this.steps ){
			log.info("()"+h.getparentstep()+ ":" + h.getchildstep());
		}
		log.info("All Hopes are manged");
	}
	public void play(SparkSession spark){
		
		/*
		 * Algorithm
		 * ---------
		 * run the workout job totally based on the hopes values. 
		 * 
		 * Priority :
		 * Steps which has 0 input hopes will be given first priority -> worked out.
		 * Steps which's previous Step's Status complete will be worked out.
		 * 
		 * Step Should run only aloud to workout only if the all count of previous Steps Completed 
		 */
		boolean still_working = false;
		log.info("STARTED:All started Started");
		do {
			for (Step s : this.steps) {
				if (!s.is_completed()) {
					
					if (s.getparentstep().size() == 0 && s.getchildstep().size() == 0) {
						log.info("0 to 0 process step found. so Simply marked as Complete");
						s.SetStatus(Status.Completed);

					} else if (this.steps.CheckparrentCompleted(s.getName())) 
					{
						log.info("As parent finished workingout on :" + s.getName());
						
						s.step.workout(this.steps.GetCompletedResult(s.getName()) , spark);
						s.SetStatus(Status.Completed);
						log.info("working result:" + s.getStatus());
					}
					else if (s.getparentstep().size() == 0){

						log.info("As its inital Step workingout on :" + s.getName());
						s.step.workout(null , spark);
						s.SetStatus(Status.Completed);
						log.info("working result:" + s.getStatus());

					}
					
				}
			}
			still_working = !this.steps.checkworking();
		} while (still_working);
		
		log.info("ENDED: All Steps are worked out");
	}

}
