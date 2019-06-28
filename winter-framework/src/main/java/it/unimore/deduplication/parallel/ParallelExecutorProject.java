package it.unimore.deduplication.parallel;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;

public class ParallelExecutorProject extends ParallelExecutor{

	@Override
	public void showResults(Collection<Correspondence<Record, Attribute>> corresp) {

        Attribute LABEL = new Attribute("label");
        Attribute ACRONYM = new Attribute("acronym");
        Attribute YEAR = new Attribute("year");
        Attribute ORGS = new Attribute("orgs");

        for (Correspondence<Record, Attribute> corr : corresp) {
            Record record1 = corr.getFirstRecord();
            Record record2 = corr.getSecondRecord();

            double sim = corr.getSimilarityScore();

            System.out.println("\n\n--------------------------------------------------------------------");
            System.out.println("R1: " + record1.toString());
            System.out.println("R2: " + record2.toString());
            System.out.println("score: " + sim);

            System.out.println("LABEL\tACRONYM\tYEAR\tORGS");
            System.out.println(record1.getValue(LABEL) + "\t" + record1.getValue(ACRONYM) + "\t" + record1.getValue(YEAR) + "\t" + record1.getValue(ORGS));
            System.out.println(record2.getValue(LABEL) + "\t" + record2.getValue(ACRONYM) + "\t" + record2.getValue(YEAR) + "\t" + record2.getValue(ORGS));

        }
	}

}
