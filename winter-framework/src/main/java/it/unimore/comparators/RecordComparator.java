package it.unimore.comparators;

import org.simmetrics.StringMetric;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.StringComparator;

public abstract class RecordComparator extends StringComparator{

	public RecordComparator(Attribute attributeRecord1, Attribute attributeRecord2) {
		super(attributeRecord1, attributeRecord2);
		// TODO Auto-generated constructor stub
	}

	@Override
	public double compare(Record record1, Record record2, Correspondence<Attribute, Matchable> schemaCorrespondence) {

		String s1 = record1.getValue(this.getAttributeRecord1());
        String s2 = record2.getValue(this.getAttributeRecord2());
        
        s1 = preprocess(s1);
        s2 = preprocess(s2);

        StringMetric m = stringMetric();
        return m.compare(s1, s2);
	}

	public abstract StringMetric stringMetric();
}
