package it.unimore.cleaning;

import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;


public interface DBCleaner {
	public void cleanDB (DataSet<Record,Attribute> dataset);

}
