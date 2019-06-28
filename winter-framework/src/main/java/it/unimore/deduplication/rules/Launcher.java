package it.unimore.deduplication.rules;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import it.unimore.comparators.RecordComparatorLabelLeveinstein;
import it.unimore.deduplication.rules.results.Result;
import it.unimore.deduplication.rules.results.ResultCorrespondance;
import it.unimore.deduplication.rules.results.ResultString;

import java.util.ArrayList;
import java.util.Collection;

import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;

public class Launcher {

	public static void main(String[] args) {
        
		//Generating records
        Attribute link = new Attribute("link");
        Attribute cc = new Attribute("cc");
        Attribute label = new Attribute("label");
        
        Record r1 = new Record("r1");
        r1.setValue(link, "www.ciao.it");
        r1.setValue(cc, "it");
        r1.setValue(label, "Ciao");
        
        Record r2 = new Record("r2");
        r2.setValue(link, "www.ciaone.it");
        r2.setValue(cc, "it");
        r2.setValue(label, "Ciaone");

        //Preparing rule parameters
        //Priority 2: if true, the rule is true. If false, continue
      	//Priority 1: if true, continue. If false end
      	//Priority 0: if true, the rule can be true. If false the rule can be false (check other parameters first)
        System.out.println("RULE PARAMETERS");
        Parameter[] p = new Parameter[3];
        p[0] = new Parameter(new RecordComparatorLabelLeveinstein(link, link), 2, 0.75);	System.out.println("If link equals, r1 and r2 are equivalent");
        p[1] = new Parameter(new RecordComparatorLabelLeveinstein(cc, cc), 1, 1); System.out.println("If cc equals, r1 and r2 can be equivalent, need to check address");
        p[2] = new Parameter(new RecordComparatorLabelLeveinstein(label, label), 0, 1); System.out.println("If label equals, r1 and r2 are equivalent\t");
        
        //Putting facts
        Facts facts = new Facts();
        facts.put("parameters", p);
        facts.put("r1", r1);
        facts.put("r2", r2);
       	
       	//Testing 
       	Collection<Correspondence<Record, Attribute>> allCorrespondances = new ArrayList<>();
       	
       	//Genereting results
       	Result r = new ResultString();
       	Result rule2 = new ResultCorrespondance(allCorrespondances,r1,r2);
       	
       	//Setting up
       	Rules rules = new Rules();
       	RulesEngine rulesEngine = new DefaultRulesEngine();
       	
       	//Firing string rule
        rules.register(new MultipleConditionRule(r));
        rulesEngine.fire(rules, facts);
        System.out.println("STRING RESULTS:");
        r.compute();
        
        //Firing correspondance rule
        rules.clear();
       	rules.register(new MultipleConditionRule(rule2));
       	rulesEngine.fire(rules, facts);
    	rule2.compute();
    	
        
        for (Correspondence<Record, Attribute> corr : allCorrespondances) {
            Record record1 = corr.getFirstRecord();
            Record record2 = corr.getSecondRecord();

            System.out.println("\n\n--------------------------------------------------------------------");
            System.out.println("R1: " + record1.toString());
            System.out.println("R2: " + record2.toString());

            System.out.println("LINK\tCOUNTRY CODE\tLABEL\t");
            System.out.println(record1.getValue(link) + "\t" + record1.getValue(cc) + "\t" + record1.getValue(label) + "\t");
            System.out.println(record2.getValue(link) + "\t" + record2.getValue(cc) + "\t" + record2.getValue(label) + "\t");

        }
	}

}
