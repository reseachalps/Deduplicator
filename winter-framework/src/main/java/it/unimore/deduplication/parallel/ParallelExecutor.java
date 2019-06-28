package it.unimore.deduplication.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;

public abstract class ParallelExecutor {
	//Create all possible pair of dataSet and run identity resolution between the pairs
	
	public void execute (List<List<DataSet<Record, Attribute>>> ALL_DATASETS, MatchingRule matchingRule, Blocker blocker, Collection<Correspondence<Record, Attribute>> allCorrespondances) throws InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<Collection<Correspondence<Record, Attribute>>>> futures = new ArrayList<Future<Collection<Correspondence<Record, Attribute>>>>();
        
		 for (List<DataSet<Record, Attribute>> allDatasets : ALL_DATASETS) {

             for (int i = 0; i < allDatasets.size(); i++) {
                 for (int j = i; j < allDatasets.size(); j++) {

                     if (i == j) {
                         continue;
                     }

                     DataSet<Record, Attribute> dataSet1 = allDatasets.get(i);
                     DataSet<Record, Attribute> dataSet2 = allDatasets.get(j);

                     DeduplicationTask dp = new DeduplicationTask(dataSet1,dataSet2,matchingRule,blocker);
                     
                     futures.add(executor.submit(dp));
                 }
             }
             
           
         }
         
         for (Future<Collection<Correspondence<Record, Attribute>>> f : futures) {
        	   Collection<Correspondence<Record, Attribute>> corresp = f.get();
        	   allCorrespondances.addAll(corresp);
        	   showResults(corresp);
 		}
         
	}
	
	//Print the results, the outcome vary according to the types of databases we have used
	public abstract void showResults(Collection<Correspondence<Record, Attribute>> corresp);

	
}
