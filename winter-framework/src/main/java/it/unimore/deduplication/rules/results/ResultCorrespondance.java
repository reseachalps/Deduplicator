package it.unimore.deduplication.rules.results;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;

public class ResultCorrespondance implements Result{
	Collection<Correspondence<Record, Attribute>> Correspondances;
	Record r1;
	Record r2;
	Attribute a;
	State succes;
	State failure ;
	State current;
	
	

	public ResultCorrespondance(Collection<Correspondence<Record, Attribute>> correspondances, Record r1, Record r2) {
		super();
		Correspondances = correspondances;
		this.r1 = r1;
		this.r2 = r2;
		
		succes = new CorrespondenceState(Correspondances,r1,r2);
		failure = new VoidState();		
				
		current = failure;
	}

	@Override
	public void setSuccesState() {
		current = succes;
	}

	@Override
	public State getResultState() {
		return current;
		
	}

	@Override
	public void setFailureState() {
		current = failure;
		
	}

	@Override
	public void compute() {
		current.behavior();
	}
	
}
