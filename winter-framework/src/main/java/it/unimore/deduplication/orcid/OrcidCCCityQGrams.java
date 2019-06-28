package it.unimore.deduplication.orcid;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.RecordBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import it.unimore.deduplication.model.Organization;

import java.util.ArrayList;
import java.util.List;

public class OrcidCCCityQGrams extends RecordBlockingKeyGenerator<Record, Attribute> {


    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences, DataIterator<Pair<String, Record>> resultCollector) {


        if (record.hasValue(Organization.CITY) && record.hasValue(Organization.COUNTRY_CODE)) {

            String city = record.getValue(Organization.CITY);
            String country_code = record.getValue(Organization.COUNTRY_CODE);
  
            city = city.replaceAll("\\(.*\\)", "").trim();
            city = city.toLowerCase();

            int Q = 3;

            for (int i = 0; i < city.length() - Q; i++) {
                resultCollector.next(new Pair<>((country_code + city.substring(i, i + Q)), record));
            }

//            System.out.println("QGRAMS of: " + label);
//            for (String q : qgrams) {
//                System.out.println("\t" + q);
//            }



        }
    }
}
