package it.unimore.deduplication;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OrganizationBlockingLabelQGrams extends RecordBlockingKeyGenerator<Record, Attribute> {


    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences, DataIterator<Pair<String, Record>> resultCollector) {


        if (record.hasValue(Organization.LABEL)) {

            String label = record.getValue(Organization.LABEL);

            label = label.replaceAll("\\(.*\\)", "").trim();

            label = label.toLowerCase();

            int Q = 3;


            String countryCode = "";
            if (record.hasValue(Organization.COUNTRY_CODE)) {
                countryCode = record.getValue(Organization.COUNTRY_CODE);
            }

            if (countryCode.equals("DE")) {
                Q = 4;
            }


            List<String> qgrams = new ArrayList<>();

//            String city = "";
//
//            if (record.hasValue(Organization.CITY)) {
//                city = record.getValue(Organization.CITY).toLowerCase();
//            }

//            for (int j = 0; j < city.length() - Q; j++) {
            for (int i = 0; i < label.length() - Q; i++) {
                String qgram = countryCode + "_" + label.substring(i, i + Q);
                qgrams.add(qgram);

            }
//            }

//            System.out.println("QGRAMS of: " + label);
//            for (String q : qgrams) {
//                System.out.println("\t" + q);
//            }


//            Set<String> stopKeys = new HashSet<>();
//            stopKeys.add("DE_log");
//            stopKeys.add("DE_log");
//            stopKeys.add("DE_olo");
//            stopKeys.add("DE_of ");
//            stopKeys.add("DE_of");
//            stopKeys.add("DE_al");
//            stopKeys.add("DE_al ");
//            stopKeys.add("DE_ati");
//            stopKeys.add("DE_for");
//            stopKeys.add("DE_tio");
//            stopKeys.add("DE_ent");
//            stopKeys.add("DE_ion");


            for (String qgram : qgrams) {

                // if (!stopKeys.contains(qgram)) {

                resultCollector.next(new Pair<>((qgram), record));
                // }
            }


        }
    }
}
