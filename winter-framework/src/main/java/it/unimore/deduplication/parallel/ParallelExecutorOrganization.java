package it.unimore.deduplication.parallel;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;

public class ParallelExecutorOrganization extends ParallelExecutor{


	@Override
	public void showResults(Collection<Correspondence<Record, Attribute>> corresp) {
	    Attribute LABEL = new Attribute("label");
        Attribute ADDRESS = new Attribute("address");
        Attribute CITY = new Attribute("city");
        Attribute LINKS = new Attribute("links");
        /*
		 for (Correspondence<Record, Attribute> corr : corresp) {
	            Record record1 = corr.getFirstRecord();
	            Record record2 = corr.getSecondRecord();

	            double sim = corr.getSimilarityScore();

	            System.out.println("\n\n--------------------------------------------------------------------");
	            System.out.println("R1: " + record1.toString());
	            System.out.println("R2: " + record2.toString());
	            System.out.println("score: " + sim);

	            System.out.println("LABEL\tADDRESS\tCITY\tLINKS");
	            System.out.println(record1.getValue(LABEL) + "\t" + record1.getValue(ADDRESS) + "\t" + record1.getValue(CITY) + "\t" + record1.getValue(LINKS));
	            System.out.println(record2.getValue(LABEL) + "\t" + record2.getValue(ADDRESS) + "\t" + record2.getValue(CITY) + "\t" + record2.getValue(LINKS));

	        }
	        */
	}
}
