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
import java.util.List;

public class OrganizationBlockingCountryCityQGrams extends RecordBlockingKeyGenerator<Record, Attribute> {
    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences, DataIterator<Pair<String, Record>> resultCollector) {
        if (record.hasValue(Organization.CITY)) {

            String city = record.getValue(Organization.CITY);

            String countryCode = "NULL";

            if (record.hasValue(Organization.COUNTRY_CODE)) {
                countryCode = record.getValue(Organization.COUNTRY_CODE);
                if (countryCode == null) {
                    System.err.println("Error NULL country code ");
                }
            } else {
                System.err.println("Error NULL country code ");
            }

            if (city != null) {

                city = city.replaceAll("\\(.*\\)", "").trim();

                city = city.toLowerCase();


                int Q = 3;

                List<String> qgrams = new ArrayList<>();
                for (int i = 0; i < city.length() - Q; i++) {
                    String qgram = city.substring(i, i + Q);
                    qgrams.add(qgram);

                }

//


                for (String qgram : qgrams) {
                    resultCollector.next(new Pair<>((qgram), record));
                }
            }

        }
    }
}
