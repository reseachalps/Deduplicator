package it.unimore.comparators;

import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.StringComparator;

public class RecordComparatorSmithWaterman extends RecordComparator{

	public RecordComparatorSmithWaterman(Attribute attributeRecord1, Attribute attributeRecord2) {
		super(attributeRecord1, attributeRecord2);
		// TODO Auto-generated constructor stub
	}

	@Override
	public StringMetric stringMetric() {
		return StringMetrics.smithWaterman();
	}

}
