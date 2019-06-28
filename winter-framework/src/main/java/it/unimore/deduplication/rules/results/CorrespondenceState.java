package it.unimore.deduplication.rules.results;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;

public class CorrespondenceState extends State{
	Collection<Correspondence<Record, Attribute>> Correspondances;
	Record r1;
	Record r2;
	
	
	
	

	public CorrespondenceState(Collection<Correspondence<Record, Attribute>> correspondances, Record r1, Record r2) {
		super();
		Correspondances = correspondances;
		this.r1 = r1;
		this.r2 = r2;
	}





	@Override
	void behavior() {
		Correspondence<Record, Attribute> c = new Correspondence<Record, Attribute>();
		c.setFirstRecord(r1);
		c.setSecondRecord(r2);
		c.setsimilarityScore(1);
		Correspondances.add(c);
	}
		
}	
