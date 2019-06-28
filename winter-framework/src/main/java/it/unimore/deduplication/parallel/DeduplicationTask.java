package it.unimore.deduplication.parallel;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.joda.time.LocalTime;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import edu.stanford.nlp.time.SUTime.Time;

public class DeduplicationTask implements Callable{
	 DataSet<Record, Attribute> dataSet1;
	 DataSet<Record, Attribute> dataSet2;
	 MatchingRule matchingRule;
	 Blocker blocker;
	 final Attribute  LABEL = new Attribute("label");
     final Attribute ADDRESS = new Attribute("address");
     final Attribute CITY = new Attribute("city");
     final Attribute LINKS = new Attribute("links");
	 
	public DeduplicationTask(DataSet<Record, Attribute> dataSet1, DataSet<Record, Attribute> dataSet2,
			MatchingRule matchingRule, Blocker blocker) {
		super();
		this.dataSet1 = dataSet1;
		this.dataSet2 = dataSet2;
		this.matchingRule = matchingRule;
		this.blocker = blocker;
	}

	@Override
	public Object call() throws Exception {

          System.out.println("Dataset1 size: " + dataSet1.size());
          System.out.println("Dataset2 size: " + dataSet2.size());

          MatchingEngine<Record, Attribute> engine = new MatchingEngine<>();
//      Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(openaire,
//              bvd, null, matchingRule, blocker);

          Processable<Correspondence<Record, Attribute>> correspondences = engine.runIdentityResolution(dataSet1,
                  dataSet2, null, matchingRule, blocker);

          System.out.println("Identity resolution between dataset1(" + dataSet1.size() + ") and dataSet2(" + dataSet2.size() + ").");
          System.out.println("Number of correspondences: " + correspondences.size());


          Collection<Correspondence<Record, Attribute>> corresp = correspondences.get();

		return corresp;
	}

}
