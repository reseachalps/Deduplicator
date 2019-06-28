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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrganizationBlockingCountryLabelQGramsChildren extends RecordBlockingKeyGenerator<Record, Attribute> {

    Map<String, Integer> idGroup = new HashMap<>();


    public OrganizationBlockingCountryLabelQGramsChildren(Map<String, Integer> idGroup) {
        this.idGroup = idGroup;
    }


    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences, DataIterator<Pair<String, Record>> resultCollector) {
        if (record.hasValue(Organization.LABEL)) {

            String label = record.getValue(Organization.LABEL);

            String countryCode = "NULL";

            if (record.hasValue(Organization.COUNTRY_CODE)) {
                countryCode = record.getValue(Organization.COUNTRY_CODE).toLowerCase();
                if (countryCode == null) {
                    System.err.println("Error NULL country code ");
                }
            } else {
                System.err.println("Error NULL country code ");
            }


            Integer group = idGroup.get(record.getValue(new Attribute("id")));
            if (group == null) {
                System.err.println("NULL GROUP ID ->\t id: " + record.getValue(new Attribute("id")) + "\tlabel: " + record.getValue(Organization.LABEL));
            }


            System.out.println("Group: " + group);

            if (label != null) {

                label = label.replaceAll("\\(.*\\)", "").trim();
                label = label.toLowerCase();
                int Q = 3;

                List<String> qgrams = new ArrayList<>();
                for (int i = 0; i < label.length() - Q; i++) {
                    String qgram = label.substring(i, i + Q);
                    qgrams.add(countryCode + "_" + qgram + "_" + group);
                }
                for (String qgram : qgrams) {
                    resultCollector.next(new Pair<>((qgram), record));
                }
            }

        }
    }
}
