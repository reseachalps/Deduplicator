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

public class OrganizationBlockingKey extends RecordBlockingKeyGenerator<Record, Attribute> {

    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences,
                                     DataIterator<Pair<String, Record>> resultCollector) {

        String postcode = null;
        String country = "XX";
        if (record.hasValue(Organization.COUNTRY_CODE)){
            country = record.getValue(Organization.COUNTRY_CODE);
        }
        if (record.hasValue(Organization.POSTCODE)) {

            postcode = record.getValue(Organization.POSTCODE);

            if (postcode.length() < 5) {
//                System.out.println("Postcode malformed: "+postcode);

                for (int i = 0; i < (5 - postcode.length()); i++) {

                    postcode = "0" + postcode;
                }

                postcode= country+postcode;

            } else if (postcode.length() > 5) {
                System.err.println("ERROR POSTCODE: " + postcode);
            } else {

            }
            //
            //

            if (postcode.length() >= 4) {
                postcode = postcode.substring(0, 2) + "XXX";
                postcode= country +postcode;
            }
//            System.out.println("NEW POSTCODE: " + postcode);


        } else {

            postcode = "00000";


            for (int i = 10; i < 100; i++) {
                String bkv = country +i + "XXX";
                resultCollector.next(new Pair<>((bkv), record));

            }
        }

        // just for test matches
        //postcode = "00000";

        String bkv = postcode;
        resultCollector.next(new Pair<>((bkv), record));

    }

}
