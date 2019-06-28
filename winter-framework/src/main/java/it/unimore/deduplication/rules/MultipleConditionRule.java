package it.unimore.deduplication.rules;
import org.jeasy.rules.annotation.Action;
import org.jeasy.rules.annotation.Condition;
import org.jeasy.rules.annotation.Fact;
import org.jeasy.rules.annotation.Rule;

import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import it.unimore.deduplication.rules.results.Result;

@Rule
public class MultipleConditionRule {
		Result r;
	
		public MultipleConditionRule(Result r) {
			super();
			this.r=r;
		}

		@Condition
		 public boolean Condition(@Fact("parameters") Parameter[] parameters,@Fact("r1") Record r1, @Fact("r2") Record r2) {
			boolean condition = true;
			for (Parameter p : parameters) {
				boolean result = comparation(p,r1,r2);
				
				if (p.getPriority() == 2) {
					if (result) return result;
					else continue;
				}
				
				else {
					if(p.getPriority() == 1) {
						if (result) continue;
						else return result;
					}
					if(p.getPriority() == 0) {
						condition = result;
					}
				}
				
			}
			
			return condition;
	    }
		
		public boolean comparation(Parameter p, Record r1, Record r2) {
			return p.getComparator().compare(r1, r2, null) >= p.getWeight();
		}
		
		@Action
		public void correspondance(){
			r.setSuccesState();
		}
}
